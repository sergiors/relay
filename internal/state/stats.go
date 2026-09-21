package state

import (
	"context"
	"database/sql"
)

// Stats is the current operational snapshot of Relay's runtime, persisted as the
// JSON payload of the single-row stats table. The monotonic counters
// (events_processed, handler success/failure, retries, dlq) survive restarts and
// always reflect the latest known totals; the gauge fields (pending_entries,
// oldest_pending_age) are point-in-time snapshots of the backlog and are
// replaced on every record.
//
// The struct is the source of truth for the payload: JSON (un)marshalling is
// centralized in stats_json.go and the stable relational metadata — the row id
// and updated_at — stays a column (UpdatedAt is excluded from the payload).
type Stats struct {
	EventsProcessedTotal    int64  `json:"events_processed_total"`
	HandlerSuccessTotal     int64  `json:"handler_success_total"`
	HandlerFailureTotal     int64  `json:"handler_failure_total"`
	RetryTotal              int64  `json:"retry_total"`
	DLQTotal                int64  `json:"dlq_total"`
	PendingEntries          int64  `json:"pending_entries"`
	OldestPendingAgeSeconds int64  `json:"oldest_pending_age_seconds"`
	UpdatedAt               string `json:"-"`
}

// RecordStats upserts the single stats row with the values the caller passes.
// It is a thin wrapper over RecordStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (c *State) RecordStats(s Stats) {
	c.RecordStatsContext(context.Background(), s)
}

// RecordStatsContext upserts the single stats row with the values the caller
// passes; the whole JSON payload is REPLACED with the marshalled struct and
// updated_at is set to now(). There is no accumulation: the worker hands over
// the CURRENT cumulative registry values for the counters and current gauge
// snapshots for the backlog, so the row always mirrors the latest known totals.
//
// Absolute snapshot semantics: each flush writes the caller's current
// cumulative values; a repeated flush with identical values is a no-op effect
// (same totals, refreshed updated_at), never double-counting. Callers must pass
// CURRENT cumulative values; the worker seeds the fresh process registry from
// this table at startup so the first snapshot never resets counters. It is
// non-fatal on error: it logs and returns.
func (c *State) RecordStatsContext(ctx context.Context, s Stats) {
	payload, err := marshalStats(s)
	if err != nil {
		c.log.Warn("State: record stats failed", "error", err)
		return
	}
	_, err = c.db.ExecContext(ctx,
		`INSERT INTO stats (id, data, updated_at) VALUES (1, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   data       = excluded.data,
		   updated_at = excluded.updated_at`,
		payload, c.nowString())
	if err != nil {
		c.log.Warn("State: record stats failed", "error", err)
	}
}

// RecordStatsSnapshot persists the whole stats snapshot in ONE short
// transaction: it first prunes orphaned function_stats rows (rows whose
// function no longer has a functions row — a function removed by RecordRemoved
// must not keep a stats row), then upserts the global single-row stats, then
// upserts each per-function row present in fns. The per-function upsert is
// conditional on the function still existing in the functions table, so a
// removed function's row is not re-created even if the caller's snapshot still
// reports its (now-stale) counters. This is the worker's flush path; it keeps
// the whole snapshot atomic and idempotent (absolute values, no deltas). It
// returns the error so the caller can bound the write with a context; the
// error is also logged here, matching the package's non-fatal style.
//
// Timestamp preservation: the four per-function Last*At fields are execution
// history, so an empty incoming value PRESERVES the persisted one rather than
// erasing it. Because each row's payload is JSON, the stored payloads are read
// once inside the transaction and merged before writing (see
// mergeFunctionStatsTimestamps); counters are never merged — they are absolute
// snapshots and always overwrite.
func (c *State) RecordStatsSnapshot(ctx context.Context, s Stats, fns []FunctionStats) error {
	globalPayload, err := marshalStats(s)
	if err != nil {
		c.log.Warn("State: flush stats snapshot failed", "error", err)
		return err
	}
	err = c.rebuildTx(ctx, func(tx *sql.Tx) error {
		// Prune orphaned function_stats rows first so a removed function's row
		// is gone before the upserts below could re-create it.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM function_stats WHERE function_name NOT IN (SELECT name FROM functions)`); err != nil {
			return err
		}
		// Read the surviving stored payloads once, for the timestamp merge.
		stored, err := c.storedFunctionStatsPayloads(ctx, tx)
		if err != nil {
			return err
		}
		// One timestamp for the whole snapshot, so the global row and every
		// per-function row share the same updated_at (the previous explicit-
		// column flush computed ts once too).
		ts := c.nowString()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO stats (id, data, updated_at) VALUES (1, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   data       = excluded.data,
			   updated_at = excluded.updated_at`,
			globalPayload, ts); err != nil {
			return err
		}
		for _, fs := range fns {
			// Counters are absolute and always replaced; an empty incoming
			// timestamp is filled from the stored payload so a flush that
			// observed nothing never erases history.
			merged := mergeFunctionStatsTimestamps(stored[fs.Function], fs)
			payload, err := marshalFunctionStats(merged)
			if err != nil {
				return err
			}
			// The EXISTS guard keeps a removed function's row from being
			// re-created even though the prune above already removed it.
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO function_stats (function_name, data, updated_at)
				 SELECT ?, ?, ?
				 WHERE EXISTS (SELECT 1 FROM functions WHERE name = ?)
				 ON CONFLICT(function_name) DO UPDATE SET
				   data       = excluded.data,
				   updated_at = excluded.updated_at`,
				fs.Function, payload, ts, fs.Function); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.log.Warn("State: flush stats snapshot failed", "error", err)
	}
	return err
}

// storedFunctionStatsPayloads reads every function_stats row's decoded payload,
// keyed by function name, inside tx. A NULL/empty column yields the zero value.
// A row whose JSON is invalid is logged (naming the function and the decode
// error) and treated as the zero value, so one corrupt row cannot abort the
// whole flush; the flush below overwrites it with the incoming absolute
// snapshot, self-healing the row.
func (c *State) storedFunctionStatsPayloads(ctx context.Context, tx *sql.Tx) (map[string]FunctionStats, error) {
	rows, err := tx.QueryContext(ctx, `SELECT function_name, data FROM function_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]FunctionStats)
	for rows.Next() {
		var name string
		var data sql.NullString
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		fs, err := unmarshalFunctionStats(data)
		if err != nil {
			c.log.Warn("State: read function stats failed", "function", name, "error", err)
			continue
		}
		out[name] = fs
	}
	return out, rows.Err()
}

// storedFunctionStatsTx reads and decodes one function_stats row's payload
// inside tx, returning the zero value when the row is absent or its data column
// is NULL/empty. An invalid payload is logged (naming the function and the
// decode error) and treated as the zero value, so the caller's incoming
// absolute snapshot still lands and self-heals the row. It is the single-row
// companion of storedFunctionStatsPayloads, shared by the standalone upsert.
func (c *State) storedFunctionStatsTx(ctx context.Context, tx *sql.Tx, name string) FunctionStats {
	var data sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT data FROM function_stats WHERE function_name = ?`, name,
	).Scan(&data)
	if err == sql.ErrNoRows {
		return FunctionStats{}
	}
	if err != nil {
		c.log.Warn("State: read function stats failed", "function", name, "error", err)
		return FunctionStats{}
	}
	fs, err := unmarshalFunctionStats(data)
	if err != nil {
		c.log.Warn("State: read function stats failed", "function", name, "error", err)
		return FunctionStats{}
	}
	return fs
}

// Stats returns the current operational snapshot, or (zero, false) when no row
// has been recorded yet or the read/decoding fails (which is logged). An empty
// or NULL payload decodes to the zero Stats; a non-empty invalid payload is
// logged with the underlying JSON error and surfaced as unreadable.
func (c *State) Stats() (Stats, bool) {
	ctx := context.Background()
	var data sql.NullString
	var updatedAt sql.NullString
	err := c.db.QueryRowContext(ctx,
		`SELECT data, updated_at FROM stats WHERE id = 1`,
	).Scan(&data, &updatedAt)
	if err == sql.ErrNoRows {
		return Stats{}, false
	}
	if err != nil {
		c.log.Warn("State: read stats failed", "error", err)
		return Stats{}, false
	}
	s, err := unmarshalStats(data)
	if err != nil {
		c.log.Warn("State: read stats failed", "error", err)
		return Stats{}, false
	}
	s.UpdatedAt = updatedAt.String
	return s, true
}

package state

import (
	"context"
	"database/sql"
)

// Stats is the current operational snapshot of Relay's runtime, persisted in the
// single-row stats table. The monotonic counters (events_processed,
// handler success/failure, retries, dlq) survive restarts and always reflect the
// latest known totals; the gauge fields (pending_entries, oldest_pending_age)
// are point-in-time snapshots of the backlog and are replaced on every record.
type Stats struct {
	EventsProcessedTotal    int64
	HandlerSuccessTotal     int64
	HandlerFailureTotal     int64
	RetryTotal              int64
	DLQTotal                int64
	PendingEntries          int64
	OldestPendingAgeSeconds int64
	UpdatedAt               string
}

// RecordStats upserts the single stats row with the values the caller passes.
// It is a thin wrapper over RecordStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (c *State) RecordStats(s Stats) {
	c.RecordStatsContext(context.Background(), s)
}

// RecordStatsContext upserts the single stats row with the values the caller
// passes; every column is REPLACED with the supplied value and updated_at is
// set to now(). There is no accumulation: the worker hands over the CURRENT
// cumulative registry values for the counters and current gauge snapshots for
// the backlog, so the row always mirrors the latest known totals.
//
// Absolute snapshot semantics: each flush writes the caller's current
// cumulative values; a repeated flush with identical values is a no-op effect
// (same totals, refreshed updated_at), never double-counting. Callers must pass
// CURRENT cumulative values; the worker seeds the fresh process registry from
// this table at startup so the first snapshot never resets counters. It is
// non-fatal on error: it logs and returns.
func (c *State) RecordStatsContext(ctx context.Context, s Stats) {
	ts := now()
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO stats
		   (id, events_processed_total, handler_success_total, handler_failure_total,
		    retry_total, dlq_total, pending_entries, oldest_pending_age_seconds, updated_at)
		 VALUES
		   (1, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   events_processed_total   = excluded.events_processed_total,
		   handler_success_total    = excluded.handler_success_total,
		   handler_failure_total    = excluded.handler_failure_total,
		   retry_total              = excluded.retry_total,
		   dlq_total                = excluded.dlq_total,
		   pending_entries          = excluded.pending_entries,
		   oldest_pending_age_seconds = excluded.oldest_pending_age_seconds,
		   updated_at               = excluded.updated_at`,
		s.EventsProcessedTotal, s.HandlerSuccessTotal, s.HandlerFailureTotal,
		s.RetryTotal, s.DLQTotal, s.PendingEntries, s.OldestPendingAgeSeconds, ts)
	if err != nil {
		c.log.Printf("state: record stats: %v", err)
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
func (c *State) RecordStatsSnapshot(ctx context.Context, s Stats, fns []FunctionStats) error {
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		// Prune orphaned function_stats rows first so a removed function's row
		// is gone before the upserts below could re-create it.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM function_stats WHERE function_name NOT IN (SELECT name FROM functions)`); err != nil {
			return err
		}
		ts := now()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO stats
			   (id, events_processed_total, handler_success_total, handler_failure_total,
			    retry_total, dlq_total, pending_entries, oldest_pending_age_seconds, updated_at)
			 VALUES
			   (1, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   events_processed_total   = excluded.events_processed_total,
			   handler_success_total    = excluded.handler_success_total,
			   handler_failure_total    = excluded.handler_failure_total,
			   retry_total              = excluded.retry_total,
			   dlq_total                = excluded.dlq_total,
			   pending_entries          = excluded.pending_entries,
			   oldest_pending_age_seconds = excluded.oldest_pending_age_seconds,
			   updated_at               = excluded.updated_at`,
			s.EventsProcessedTotal, s.HandlerSuccessTotal, s.HandlerFailureTotal,
			s.RetryTotal, s.DLQTotal, s.PendingEntries, s.OldestPendingAgeSeconds, ts); err != nil {
			return err
		}
		for _, fs := range fns {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO function_stats
				   (function_name, events_processed_total, handler_success_total,
				    handler_failure_total, retry_total, dlq_total, updated_at)
				 SELECT ?, ?, ?, ?, ?, ?, ?
				 WHERE EXISTS (SELECT 1 FROM functions WHERE name = ?)
				 ON CONFLICT(function_name) DO UPDATE SET
				   events_processed_total  = excluded.events_processed_total,
				   handler_success_total   = excluded.handler_success_total,
				   handler_failure_total   = excluded.handler_failure_total,
				   retry_total             = excluded.retry_total,
				   dlq_total               = excluded.dlq_total,
				   updated_at              = excluded.updated_at`,
				fs.Function, fs.EventsProcessedTotal, fs.HandlerSuccessTotal,
				fs.HandlerFailureTotal, fs.RetryTotal, fs.DLQTotal, ts, fs.Function); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.log.Printf("state: flush stats snapshot: %v", err)
	}
	return err
}

// Stats returns the current operational snapshot, or (zero, false) when no row
// has been recorded yet or the read fails (which is logged). Columns default to
// 0 so an absent column never surfaces as a spurious value.
func (c *State) Stats() (Stats, bool) {
	ctx := context.Background()
	var s Stats
	err := c.db.QueryRowContext(ctx,
		`SELECT events_processed_total, handler_success_total, handler_failure_total,
		        retry_total, dlq_total, pending_entries, oldest_pending_age_seconds, updated_at
		 FROM stats WHERE id = 1`,
	).Scan(&s.EventsProcessedTotal, &s.HandlerSuccessTotal, &s.HandlerFailureTotal,
		&s.RetryTotal, &s.DLQTotal, &s.PendingEntries, &s.OldestPendingAgeSeconds, &s.UpdatedAt)
	if err == sql.ErrNoRows {
		return Stats{}, false
	}
	if err != nil {
		c.log.Printf("state: read stats: %v", err)
		return Stats{}, false
	}
	return s, true
}

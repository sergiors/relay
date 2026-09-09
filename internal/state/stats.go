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

// RecordStats upserts the single stats row with the values the
// caller passes; every column is REPLACED with the supplied value and
// updated_at is set to now(). There is no accumulation: the worker hands over
// the CURRENT cumulative registry values for the counters and current gauge
// snapshots for the backlog, so the row always mirrors the latest known totals.
// Callers must pass CURRENT cumulative values; the worker seeds the fresh
// process registry from this table at startup so the first snapshot never
// resets counters. It is non-fatal on error: it logs and returns.
func (c *State) RecordStats(s Stats) {
	ctx := context.Background()
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

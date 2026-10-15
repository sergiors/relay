package state

import (
	"context"
	"database/sql"
	"sort"
)

// Stats is the current operational snapshot of Relay's runtime, persisted as the
// JSONB payload of a per-worker row in the stats table (keyed by worker
// identity). The monotonic counters (events received/matched/unmatched, handler
// success/failure, retries, dlq) survive restarts and always reflect the latest
// known totals; the gauge fields (pending_entries, oldest_pending_age) are
// point-in-time snapshots of the backlog and are replaced on every record.
//
// The three event counters form a closed partition of the logical incoming
// events the runner handled: EventsReceivedTotal == EventsMatchedTotal +
// EventsUnmatchedTotal. Each logical event is classified exactly once across
// redeliveries and retries; schedule occurrences are excluded (they bypass
// event matching).
//
// Ownership: each worker writes only its OWN row (State.owner), so N workers
// sharing one PostgreSQL database keep N independent counter sets instead of
// last-writer-wins overwriting a single global row. A worker's startup restore
// reads only its own row (OwnerStats); CLI and observability reads aggregate
// across every owner (Stats) via mergeStats.
//
// The struct is the source of truth for the payload: JSON (un)marshalling is
// centralized in stats_json.go and the stable relational metadata — the worker
// key and updated_at — stays a column (UpdatedAt is excluded from the payload).
type Stats struct {
	EventsReceivedTotal     int64  `json:"events_received_total"`
	EventsMatchedTotal      int64  `json:"events_matched_total"`
	EventsUnmatchedTotal    int64  `json:"events_unmatched_total"`
	HandlerSuccessTotal     int64  `json:"handler_success_total"`
	HandlerFailureTotal     int64  `json:"handler_failure_total"`
	RetryTotal              int64  `json:"retry_total"`
	DLQTotal                int64  `json:"dlq_total"`
	PendingEntries          int64  `json:"pending_entries"`
	OldestPendingAgeSeconds int64  `json:"oldest_pending_age_seconds"`
	UpdatedAt               string `json:"-"`
}

// RecordStats upserts this worker's stats row with the values the caller passes.
// It is a thin wrapper over RecordStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (st *State) RecordStats(stats Stats) {
	st.RecordStatsContext(context.Background(), stats)
}

// RecordStatsContext upserts THIS worker's stats row with the values the caller
// passes; the whole JSONB payload is REPLACED with the marshalled struct and
// updated_at is set to now(). There is no accumulation: the worker hands over
// the CURRENT cumulative registry values for the counters and current gauge
// snapshots for the backlog, so the row always mirrors the latest known totals.
//
// The payload is written with PostgreSQL's jsonb type, which validates it at
// write time.
//
// Absolute snapshot semantics: each flush writes the caller's current
// cumulative values; a repeated flush with identical values is a no-op effect
// (same totals, refreshed updated_at), never double-counting. Callers must pass
// CURRENT cumulative values; the worker seeds the fresh process registry from
// this row at startup so the first snapshot never resets counters. It is
// non-fatal on error: it logs and returns.
func (st *State) RecordStatsContext(ctx context.Context, stats Stats) {
	payload, err := marshalStats(stats)
	if err != nil {
		st.log.Warn("State: record stats failed", "error", err)
		return
	}
	_, err = st.db.ExecContext(ctx,
		`INSERT INTO stats (worker, data, updated_at) VALUES ($1, $2, $3)
		 ON CONFLICT(worker) DO UPDATE SET
		   data       = excluded.data,
		   updated_at = excluded.updated_at`,
		st.owner, payload, st.nowString())
	if err != nil {
		st.log.Warn("State: record stats failed", "error", err)
	}
}

// RecordStatsSnapshot persists the whole stats snapshot in ONE short
// transaction: it first prunes orphaned app_stats rows (rows whose
// app no longer has an apps row — an app removed by RecordRemoved
// must not keep a stats row), then upserts this worker's global stats row, then
// upserts each per-app row present in fns for THIS worker. The per-app upsert is
// conditional on the app still existing in the apps table, so a
// removed app's row is not re-created even if the caller's snapshot still
// reports its (now-stale) counters. This is the worker's flush path; it keeps
// the whole snapshot atomic and idempotent (absolute values, no deltas). It
// returns the error so the caller can bound the write with a context; the
// error is also logged here, matching the package's non-fatal style.
//
// Timestamp preservation: the four per-app Last*At fields are execution
// history, so an empty incoming value PRESERVES the persisted one rather than
// erasing it. Only THIS worker's stored payloads are read for the merge (the
// row is keyed by app+worker), so a worker never inherits another worker's
// timestamps. Because each row's payload is JSONB, the stored payloads are read
// once inside the transaction and merged before writing (see
// mergeAppStatsTimestamps); counters are never merged — they are absolute
// snapshots and always overwrite.
func (st *State) RecordStatsSnapshot(ctx context.Context, stats Stats, fns []AppStats) error {
	globalPayload, err := marshalStats(stats)
	if err != nil {
		st.log.Warn("State: flush stats snapshot failed", "error", err)
		return err
	}
	err = st.rebuildTx(ctx, func(tx *sql.Tx) error {
		// Prune orphaned app_stats rows first so a removed app's row
		// is gone before the upserts below could re-create it. The apps table is
		// shared and authoritative across workers, so an orphan is an orphan for
		// every owner.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM app_stats WHERE app_name NOT IN (SELECT name FROM apps)`); err != nil {
			return err
		}
		// Read THIS worker's surviving stored payloads once, for the timestamp
		// merge.
		stored, err := st.storedAppStatsPayloads(ctx, tx)
		if err != nil {
			return err
		}
		// One timestamp for the whole snapshot, so this worker's global row and
		// every per-app row share the same updated_at.
		ts := st.nowString()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO stats (worker, data, updated_at) VALUES ($1, $2, $3)
			 ON CONFLICT(worker) DO UPDATE SET
			   data       = excluded.data,
			   updated_at = excluded.updated_at`,
			st.owner, globalPayload, ts); err != nil {
			return err
		}
		for _, fs := range fns {
			// Counters are absolute and always replaced; an empty incoming
			// timestamp is filled from the stored payload so a flush that
			// observed nothing never erases history.
			merged := mergeAppStatsTimestamps(stored[fs.App], fs)
			payload, err := marshalAppStats(merged)
			if err != nil {
				return err
			}
			// The EXISTS guard keeps a removed app's row from being
			// re-created even though the prune above already removed it. The
			// SELECT form lets the guard and the upsert stay one statement.
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO app_stats (app_name, worker, data, updated_at)
				 SELECT $1, $2, $3, $4
				 WHERE EXISTS (SELECT 1 FROM apps WHERE name = $1)
				 ON CONFLICT(app_name, worker) DO UPDATE SET
				   data       = excluded.data,
				   updated_at = excluded.updated_at`,
				fs.App, st.owner, payload, ts); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		st.log.Warn("State: flush stats snapshot failed", "error", err)
	}
	return err
}

// storedAppStatsPayloads reads THIS worker's app_stats row payloads, keyed by
// app name, inside tx. A row whose JSON is invalid is logged (naming the app and
// the decode error) and treated as the zero value, so one corrupt row cannot
// abort the whole flush; the flush below overwrites it with the incoming
// absolute snapshot, self-healing the row.
func (st *State) storedAppStatsPayloads(ctx context.Context, tx *sql.Tx) (map[string]AppStats, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT app_name, `+jsonPayloadExpr+` FROM app_stats WHERE worker = $1`, st.owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]AppStats)
	for rows.Next() {
		var name string
		var data sql.NullString
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		fs, err := unmarshalAppStats(data)
		if err != nil {
			st.log.Warn("State: read app stats failed", "app", name, "error", err)
			continue
		}
		out[name] = fs
	}
	return out, rows.Err()
}

// storedAppStatsTx reads and decodes THIS worker's app_stats row payload inside
// tx, returning the zero value when the row is absent. An invalid payload is
// logged (naming the app and the decode error) and treated as the zero value, so
// the caller's incoming absolute snapshot still lands and self-heals the row. It
// is the single-row companion of storedAppStatsPayloads, shared by the
// standalone upsert.
func (st *State) storedAppStatsTx(ctx context.Context, tx *sql.Tx, name string) AppStats {
	var data sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+` FROM app_stats WHERE app_name = $1 AND worker = $2`, name, st.owner,
	).Scan(&data)
	if err == sql.ErrNoRows {
		return AppStats{}
	}
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}
	}
	fs, err := unmarshalAppStats(data)
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}
	}
	return fs
}

// ResetStats returns the persisted cumulative statistics to their
// fresh-install state in ONE transaction while PRESERVING the shape of the
// stored rows: every worker's global stats row and every per-app app_stats row
// survive, decoded from their typed JSON payloads. The global rows' seven
// cumulative counters (events received/matched/unmatched and
// handler success/handler failure/retry/DLQ) are zeroed; each app row's
// event-matched/handler counters plus the cumulative
// warm-acquire/cold-start/discarded pool counters are zeroed, and its four
// Last*At execution-history timestamps are cleared. The rows themselves are
// rewritten in place (an UPDATE, never a DELETE), so a running worker's flush
// simply writes new absolute values over them instead of re-creating them.
//
// The reset affects EVERY owner: the persisted history is a shared operator
// view, so `relay stats reset` clears all workers' rows (a running worker resets
// its own in-memory source over the socket; the CLI fallback resets the whole
// database).
//
// The global backlog gauges (pending_entries / oldest_pending_age_seconds) are
// POINT-IN-TIME snapshots of the live Redis backlog, deliberately NOT reset:
// zeroing them would misreport a backlog that still exists — the next worker
// flush replaces them with fresh values anyway. Fields the typed marshal model
// does not know about are not round-tripped (the typed structs are the source
// of truth); every KNOWN unrelated field survives. updated_at is refreshed to
// now() on every rewritten row (generic last-write metadata, same semantics as
// RecordStats). Everything else in the database (apps and their
// snapshots, git state, secrets, invocation state) is untouched; no Prometheus
// counter, Redis state, worker, or container is involved.
//
// Concurrency: one transaction (rebuildTx), so a partial reset never lands. The
// in-memory metrics registry is NOT touched here — a running worker must reset
// its own in-memory source under its flush mutex so a captured pre-reset
// snapshot cannot be written after this reset (see worker's stats resetter);
// this method is the complete reset for a stopped worker.
//
// A corrupt payload — global or per-app — is fatal: it cannot be decoded
// and rewritten to zero, so the decode error is returned and the transaction
// rolls back, leaving every row exactly as it was. A partial reset is never
// mistaken for a complete one.
//
// Returns the error so the CLI can surface it; the package's Warn log also
// records the failure, matching RecordStats.
func (st *State) ResetStats() error {
	ctx := context.Background()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		// One timestamp for every rewritten row, so every global row and all
		// per-app rows share it (the flush snapshot does the same).
		ts := st.nowString()
		if err := st.resetGlobalStatsTx(ctx, tx, ts); err != nil {
			return err
		}
		return st.resetAppStatsTx(ctx, tx, ts)
	})
	if err != nil {
		st.log.Warn("State: reset stats failed", "error", err)
	}
	return err
}

// resetGlobalStatsTx zeroes every worker's stats row cumulative counters in
// tx while preserving the live backlog gauges and any other decoded field. An
// absent table (no rows) has nothing cumulative to zero and is left untouched
// (creating a row would falsely claim stats were recorded). A corrupt payload is
// returned as an error so the transaction rolls back.
func (st *State) resetGlobalStatsTx(ctx context.Context, tx *sql.Tx, ts string) error {
	rows, err := tx.QueryContext(ctx, `SELECT worker, `+jsonPayloadExpr+` FROM stats`)
	if err != nil {
		return err
	}
	type globalRow struct {
		worker string
		data   sql.NullString
	}
	var pending []globalRow
	for rows.Next() {
		var r globalRow
		if err := rows.Scan(&r.worker, &r.data); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, r := range pending {
		// The column is NOT NULL, so a NULL rendering means the stored value is
		// not the typed payload; unmarshalStats surfaces that as a decode failure
		// (rolling the reset back) rather than silently zeroing a corrupt row.
		stats, err := unmarshalStats(r.data)
		if err != nil {
			return err
		}
		// Zero ONLY the cumulative counters; the two gauges are live backlog
		// snapshots and are preserved (the next flush refreshes them).
		stats.EventsReceivedTotal = 0
		stats.EventsMatchedTotal = 0
		stats.EventsUnmatchedTotal = 0
		stats.HandlerSuccessTotal = 0
		stats.HandlerFailureTotal = 0
		stats.RetryTotal = 0
		stats.DLQTotal = 0
		payload, err := marshalStats(stats)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE stats SET data = $1, updated_at = $2 WHERE worker = $3`,
			payload, ts, r.worker); err != nil {
			return err
		}
	}
	return nil
}

// resetAppStatsTx zeroes every app_stats row's cumulative fields in
// tx with an UPDATE per row (never a DELETE): the event-matched/handler
// counters, the three warm-container pool counters, and the four Last*At
// timestamps. A corrupt payload returns the decode error, rolling the whole
// reset back. Timestamps are cleared (not merged) because a reset is an explicit
// erasure of execution history. It covers every worker's row.
func (st *State) resetAppStatsTx(ctx context.Context, tx *sql.Tx, ts string) error {
	type row struct {
		name   string
		worker string
		data   sql.NullString
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT app_name, worker, `+jsonPayloadExpr+` FROM app_stats`)
	if err != nil {
		return err
	}
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.name, &r.worker, &r.data); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, r := range pending {
		fs, err := unmarshalAppStats(r.data)
		if err != nil {
			return err
		}
		fs.EventsMatchedTotal = 0
		fs.HandlerSuccessTotal = 0
		fs.HandlerFailureTotal = 0
		fs.RetryTotal = 0
		fs.DLQTotal = 0
		fs.WarmAcquiresTotal = 0
		fs.ColdStartsTotal = 0
		fs.DiscardedTotal = 0
		fs.LastExecutionAt = ""
		fs.LastSuccessAt = ""
		fs.LastFailureAt = ""
		fs.LastDLQAt = ""
		payload, err := marshalAppStats(fs)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE app_stats SET data = $1, updated_at = $2 WHERE app_name = $3 AND worker = $4`,
			payload, ts, r.name, r.worker); err != nil {
			return err
		}
	}
	return nil
}

// Stats returns the operational snapshot AGGREGATED across every worker owner,
// or (zero, false) when no row has been recorded yet or the read/decoding fails
// (which is logged). Cumulative counters are summed across workers; the two
// backlog gauges (a shared Redis backlog each worker reports) come from the
// FRESHEST worker snapshot (the newest updated_at), so a dead worker's stale
// gauge cannot mask the live value; UpdatedAt is the newest. It is the read the
// CLI and observability use; a worker restoring its own counters uses OwnerStats
// instead.
func (st *State) Stats() (Stats, bool) {
	return st.aggregateStats(false, "")
}

// OwnerStats returns THIS worker's operational snapshot, or (zero, false) when
// this worker has no row yet. It is the read a worker uses at startup to restore
// its own counters (never another worker's), so restarts of the same identity
// continue its totals without double-counting a shared database.
func (st *State) OwnerStats() (Stats, bool) {
	return st.aggregateStats(true, st.owner)
}

// aggregateStats reads stats rows and merges them. When byOwner is true only
// rows for owner are read (the worker-scoped restore); otherwise every row is
// read and merged (the cross-worker aggregate).
func (st *State) aggregateStats(byOwner bool, owner string) (Stats, bool) {
	ctx := context.Background()
	query := `SELECT ` + jsonPayloadExpr + `, updated_at FROM stats`
	args := []any(nil)
	if byOwner {
		query += ` WHERE worker = $1`
		args = append(args, owner)
	}
	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		st.log.Warn("State: read stats failed", "error", err)
		return Stats{}, false
	}
	defer rows.Close()

	var (
		out  Stats
		seen bool
	)
	for rows.Next() {
		var data, updatedAt sql.NullString
		if err := rows.Scan(&data, &updatedAt); err != nil {
			st.log.Warn("State: read stats failed", "error", err)
			return out, seen
		}
		s, err := unmarshalStats(data)
		if err != nil {
			st.log.Warn("State: read stats failed", "error", err)
			continue
		}
		s.UpdatedAt = updatedAt.String
		mergeStats(&out, s)
		seen = true
	}
	if !seen {
		return Stats{}, false
	}
	return out, true
}

// mergeStats folds src into dst: cumulative counters are summed (each row is an
// independent worker's absolute total), while the two backlog gauges are NOT
// summed and NOT maxed. Every worker snapshots the SAME shared Redis backlog, so
// the aggregate must present exactly ONE worker's observation — the FRESHEST one
// (the row with the newest updated_at), which is the worker currently flushing.
// Taking the maximum instead would let a dead worker's stale high gauge mask the
// live value indefinitely. UpdatedAt takes the newest, and the freshest row's
// gauges win; on an updated_at tie (RFC3339 has second granularity) the larger
// gauge wins, so the aggregate is deterministic regardless of row order and never
// under-reports an equally-fresh backlog.
func mergeStats(dst *Stats, src Stats) {
	dst.EventsReceivedTotal += src.EventsReceivedTotal
	dst.EventsMatchedTotal += src.EventsMatchedTotal
	dst.EventsUnmatchedTotal += src.EventsUnmatchedTotal
	dst.HandlerSuccessTotal += src.HandlerSuccessTotal
	dst.HandlerFailureTotal += src.HandlerFailureTotal
	dst.RetryTotal += src.RetryTotal
	dst.DLQTotal += src.DLQTotal
	switch {
	case src.UpdatedAt > dst.UpdatedAt:
		// A strictly fresher snapshot owns the gauges outright.
		dst.PendingEntries = src.PendingEntries
		dst.OldestPendingAgeSeconds = src.OldestPendingAgeSeconds
	case src.UpdatedAt == dst.UpdatedAt:
		// Equally fresh: take the larger gauge so the result is order-independent.
		if src.PendingEntries > dst.PendingEntries {
			dst.PendingEntries = src.PendingEntries
		}
		if src.OldestPendingAgeSeconds > dst.OldestPendingAgeSeconds {
			dst.OldestPendingAgeSeconds = src.OldestPendingAgeSeconds
		}
	}
	dst.UpdatedAt = maxRFC3339(dst.UpdatedAt, src.UpdatedAt)
}

// maxRFC3339 returns the later of two RFC3339 UTC strings. RFC3339 UTC strings
// are lexicographically ordered, and the empty string ("never") sorts first.
func maxRFC3339(a, b string) string {
	if b > a {
		return b
	}
	return a
}

// mergeAppStatsInto folds src into dst for one app: cumulative counters are
// summed across workers, and the four Last*At execution-history timestamps take
// the newest observation. UpdatedAt takes the newest.
func mergeAppStatsInto(dst *AppStats, src AppStats) {
	dst.EventsMatchedTotal += src.EventsMatchedTotal
	dst.HandlerSuccessTotal += src.HandlerSuccessTotal
	dst.HandlerFailureTotal += src.HandlerFailureTotal
	dst.RetryTotal += src.RetryTotal
	dst.DLQTotal += src.DLQTotal
	dst.WarmAcquiresTotal += src.WarmAcquiresTotal
	dst.ColdStartsTotal += src.ColdStartsTotal
	dst.DiscardedTotal += src.DiscardedTotal
	dst.LastExecutionAt = maxRFC3339(dst.LastExecutionAt, src.LastExecutionAt)
	dst.LastSuccessAt = maxRFC3339(dst.LastSuccessAt, src.LastSuccessAt)
	dst.LastFailureAt = maxRFC3339(dst.LastFailureAt, src.LastFailureAt)
	dst.LastDLQAt = maxRFC3339(dst.LastDLQAt, src.LastDLQAt)
	dst.UpdatedAt = maxRFC3339(dst.UpdatedAt, src.UpdatedAt)
}

// sortAppStats orders app stats by app name, so aggregated reads are
// deterministic (the SQL ORDER BY the previous queries carried).
func sortAppStats(out []AppStats) {
	sort.Slice(out, func(i, j int) bool { return out[i].App < out[j].App })
}

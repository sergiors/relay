package state

import (
	"context"
	"database/sql"
)

// AppStats is the per-app operational snapshot persisted as the JSON
// payload of the app_stats table. Unlike the single-row global Stats, it is
// keyed by app name (the relational app_stats.app_name column) so
// each app's payload is attributed independently.
//
// Semantics (see runner.Handle): EventsMatchedTotal counts an app once per
// logical event for which at least one of its rules matched — a
// apps-engaged counter, distinct from the global events_matched_total (an
// event matching two apps counts once globally, once per app here).
// It is classified once per logical event across redeliveries. A handler
// failure does not move the event out of the matched class.
// HandlerSuccessTotal/HandlerFailureTotal are per rule execution.
// RetryTotal counts every failing rule execution (a handler retry, not a
// stream message reclaim); DLQTotal counts the exhaustion COMMIT for an
// invocation: an app is counted once when its failing rule execution is
// the one that exhausts its retry budget and the invocation's terminal
// exhausted marker is committed. It is counted at that exhaustion commit,
// which may precede the actual message-level DLQ write (that happens after
// every exhausted sibling invocation in the same delivery has finished) and
// which may never be followed by one if that write fails — so it counts
// invocations exhausted, not successfully written DLQ entries.
//
// The four Last*At fields are per-app execution-history timestamps in the
// RFC3339 convention of UpdatedAt (empty string = never observed):
// LastExecutionAt is the last handler-execution attempt (retries count — every
// claimed attempt is an execution); LastSuccessAt / LastFailureAt the last
// successful / failed handler execution (a failed attempt that will retry
// counts as a failure); LastDLQAt the last invocation that exhausted its
// retries (the exhaustion commit — the last retry exhaustion, not a
// successfully written DLQ entry, and not every failure). They are reset by
// nothing but a genuine removal: unlike the counters, an incoming empty value
// must never clobber a persisted timestamp (see mergeAppStatsTimestamps).
//
// WarmAcquiresTotal / ColdStartsTotal / DiscardedTotal are the cumulative
// warm-container pool counters (see runtime.PoolSnapshot): warm acquires served
// by an existing idle container, cold starts that created a fresh container,
// and container discards (all reasons summed). They are cumulative absolute
// totals like the other counters, persisted so the standalone
// `relay app inspect` process can render the Runtime pool section without
// access to the worker's in-memory pool. The LIVE pool gauges (capacity,
// container counts by lease state) are deliberately NOT persisted: a persisted
// live gauge would go stale between flushes.
//
// The struct is the source of truth for the payload. App and UpdatedAt are
// stable relational metadata (app_stats.app_name / .updated_at) and
// are excluded from the JSON by their json:"-" tags; JSON (un)marshalling is
// centralized in stats_json.go.
type AppStats struct {
	App string `json:"-"`
	// Counters are absolute snapshots: always emitted (even when 0) so a
	// legitimate reset is representable. The Last*At fields are omitted when
	// empty, which is what lets an empty incoming value preserve the persisted
	// timestamp during the flush merge.
	EventsMatchedTotal  int64  `json:"events_matched_total"`
	HandlerSuccessTotal int64  `json:"handler_success_total"`
	HandlerFailureTotal int64  `json:"handler_failure_total"`
	RetryTotal          int64  `json:"retry_total"`
	DLQTotal            int64  `json:"dlq_total"`
	WarmAcquiresTotal   int64  `json:"warm_acquires_total"`
	ColdStartsTotal     int64  `json:"cold_starts_total"`
	DiscardedTotal      int64  `json:"discarded_total"`
	LastExecutionAt     string `json:"last_execution_at,omitempty"`
	LastSuccessAt       string `json:"last_success_at,omitempty"`
	LastFailureAt       string `json:"last_failure_at,omitempty"`
	LastDLQAt           string `json:"last_dlq_at,omitempty"`
	UpdatedAt           string `json:"-"`
}

// RecordAppStats upserts the app_stats row for appStats.App. It is a
// thin wrapper over RecordAppStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (st *State) RecordAppStats(appStats AppStats) {
	st.RecordAppStatsContext(context.Background(), appStats)
}

// RecordAppStatsContext upserts the app_stats row for appStats.App,
// replacing the JSON payload with the supplied value and setting updated_at to
// now(). The payload is written as SQLite binary JSON via jsonb(?). Counters
// behave as an absolute snapshot (see RecordStats); the four Last*At timestamps
// are additionally merged so an EMPTY incoming value preserves the previously
// stored timestamp instead of overwriting it with "" — "no observation" must
// never erase "last observed at". The read and write happen in one transaction
// (the same short-transaction pattern as RecordStatsSnapshot), so the merge is
// atomic with respect to other writers. Callers must pass CURRENT cumulative
// values; the worker seeds the fresh process registry from this table at
// startup so the first snapshot never resets counters. It is non-fatal on
// error: it logs and returns.
func (st *State) RecordAppStatsContext(ctx context.Context, appStats AppStats) {
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		stored := st.storedAppStatsTx(ctx, tx, appStats.App)
		merged := mergeAppStatsTimestamps(stored, appStats)
		payload, err := marshalAppStats(merged)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO app_stats (app_name, data, updated_at) VALUES (?, jsonb(?), ?)
			 ON CONFLICT(app_name) DO UPDATE SET
			   data       = excluded.data,
			   updated_at = excluded.updated_at`,
			appStats.App, payload, st.nowString())
		return err
	})
	if err != nil {
		st.log.Warn("State: record app stats failed", "app", appStats.App, "error", err)
	}
}

// AppStats returns the per-app snapshot for name, or (zero, false)
// when no row has been recorded yet or the read/decoding fails (which is
// logged). The App field is set to name on success (it is relational
// metadata, not part of the JSON payload).
func (st *State) AppStats(name string) (AppStats, bool) {
	ctx := context.Background()
	var data sql.NullString
	var updatedAt sql.NullString
	err := st.db.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+`, updated_at FROM app_stats WHERE app_name = ?`, name,
	).Scan(&data, &updatedAt)
	if err == sql.ErrNoRows {
		return AppStats{}, false
	}
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}, false
	}
	appStats, err := unmarshalAppStats(data)
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}, false
	}
	appStats.App = name
	appStats.UpdatedAt = updatedAt.String
	return appStats, true
}

// AppNames returns the names of every app row, ordered by name, and
// whether the read succeeded. It is the live-set reader the worker's flush
// sweep consults to know which apps currently exist, so its metrics
// registry can drop series for apps that no longer exist (see
// metrics.SweepAppMetrics). A removed app has no apps row, so
// it is absent here and its stale series are swept. The second return is the
// ok flag: (names, true) on success, (nil, false) on error (which is logged).
// The worker's sweep treats a failed read as an unknown live set and must fail
// OPEN — retain every series; the next successful flush sweeps — because
// sweeping with an empty set would delete live apps' series. This is why
// callers must distinguish an empty-but-known live set from a failed read.
func (st *State) AppNames() ([]string, bool) {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx, `SELECT name FROM apps ORDER BY name`)
	if err != nil {
		st.log.Warn("State: list app names failed", "error", err)
		return nil, false
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			st.log.Warn("State: scan app name failed", "error", err)
			return out, false
		}
		out = append(out, name)
	}
	return out, true
}

// AllAppStats returns every app_stats row ordered by app name.
// The worker uses it at startup to restore counters for apps that have
// persisted stats even when the fresh registry snapshot is empty (e.g. a
// app idle this process but active last process). The App field is
// taken from the relational key column (not the JSON payload). A row whose JSON
// is invalid is logged and skipped (its stats are unknowable); it returns nil
// on a read error, matching the file's non-fatal style.
func (st *State) AllAppStats() []AppStats {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx,
		`SELECT app_name, `+jsonPayloadExpr+`, updated_at FROM app_stats ORDER BY app_name`)
	if err != nil {
		st.log.Warn("State: list app stats failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []AppStats
	for rows.Next() {
		var name string
		var data, updatedAt sql.NullString
		if err := rows.Scan(&name, &data, &updatedAt); err != nil {
			st.log.Warn("State: scan app stats failed", "error", err)
			return out
		}
		appStats, err := unmarshalAppStats(data)
		if err != nil {
			st.log.Warn("State: read app stats failed", "app", name, "error", err)
			continue
		}
		appStats.App = name
		appStats.UpdatedAt = updatedAt.String
		out = append(out, appStats)
	}
	return out
}

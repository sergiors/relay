package state

import (
	"context"
	"database/sql"
)

// AppStats is the per-app operational snapshot persisted as the JSONB
// payload of an app_stats row, keyed by (app name, worker identity) so each
// app's payload is attributed independently both per app and per owning worker.
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
// stable relational metadata (app_stats.app_name / .updated_at)
// and are excluded from the JSON by their json:"-" tags; JSON (un)marshalling is
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

// RecordAppStats upserts THIS worker's app_stats row for appStats.App. It is a
// thin wrapper over RecordAppStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (st *State) RecordAppStats(appStats AppStats) {
	st.RecordAppStatsContext(context.Background(), appStats)
}

// RecordAppStatsContext upserts THIS worker's app_stats row for appStats.App,
// replacing the JSONB payload with the supplied value and setting updated_at to
// now(). The payload is written with PostgreSQL's jsonb type. Counters
// behave as an absolute snapshot (see RecordStats); the four Last*At timestamps
// are additionally merged so an EMPTY incoming value preserves the previously
// stored timestamp instead of overwriting it with "" — "no observation" must
// never erase "last observed at". The read and write happen in one transaction
// (the same short-transaction pattern as RecordStatsSnapshot), so the merge is
// atomic with respect to other writers. Callers must pass CURRENT cumulative
// values; the worker seeds the fresh process registry from its own rows at
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
			`INSERT INTO app_stats (app_name, worker, data, updated_at) VALUES ($1, $2, $3, $4)
			 ON CONFLICT(app_name, worker) DO UPDATE SET
			   data       = excluded.data,
			   updated_at = excluded.updated_at`,
			appStats.App, st.owner, payload, st.nowString())
		return err
	})
	if err != nil {
		st.log.Warn("State: record app stats failed", "app", appStats.App, "error", err)
	}
}

// AppStats returns the per-app snapshot for name AGGREGATED across every worker
// owner, or (zero, false) when no row has been recorded yet or the read/decoding
// fails (which is logged). Counters are summed and the Last*At timestamps take
// the newest observation. The App field is set to name on success (it is
// relational metadata, not part of the JSON payload). A worker restoring its own
// per-app counters uses OwnerAppStats instead.
func (st *State) AppStats(name string) (AppStats, bool) {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx,
		`SELECT `+jsonPayloadExpr+`, updated_at FROM app_stats WHERE app_name = $1`, name)
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}, false
	}
	defer rows.Close()

	var (
		out  AppStats
		seen bool
	)
	for rows.Next() {
		var data, updatedAt sql.NullString
		if err := rows.Scan(&data, &updatedAt); err != nil {
			st.log.Warn("State: read app stats failed", "app", name, "error", err)
			return out, seen
		}
		fs, err := unmarshalAppStats(data)
		if err != nil {
			st.log.Warn("State: read app stats failed", "app", name, "error", err)
			continue
		}
		fs.UpdatedAt = updatedAt.String
		mergeAppStatsInto(&out, fs)
		seen = true
	}
	if !seen {
		return AppStats{}, false
	}
	out.App = name
	return out, true
}

// OwnerAppStats returns every app_stats row owned by THIS worker, grouped by
// app and ordered by app name. It is the read a worker uses at startup to restore
// its own per-app counters (never another worker's), so a restart of the same
// identity continues its totals. A row whose JSON is invalid is logged and
// skipped.
func (st *State) OwnerAppStats() []AppStats {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx,
		`SELECT app_name, `+jsonPayloadExpr+`, updated_at FROM app_stats WHERE worker = $1 ORDER BY app_name`,
		st.owner)
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
// A failure part-way through iteration is a failed read, not a shorter live
// set: partial names are never reported as a complete answer. ctx bounds the
// read (the worker passes its flush context), so a cancelled flush cannot
// block the sweep on the database.
func (st *State) AppNames(ctx context.Context) ([]string, bool) {
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
			return nil, false
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		st.log.Warn("State: list app names failed", "error", err)
		return nil, false
	}
	return out, true
}

// AllAppStats returns every app_stats row AGGREGATED across worker owners,
// grouped by app and ordered by app name. Counters are summed and Last*At
// timestamps take the newest observation. It is the cross-worker read used by
// the CLI/operator view; a worker restoring its own counters uses OwnerAppStats.
// A row whose JSON is invalid is logged and skipped (its stats are unknowable);
// it returns nil on a read error, matching the file's non-fatal style.
func (st *State) AllAppStats() []AppStats {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx,
		`SELECT app_name, `+jsonPayloadExpr+`, updated_at FROM app_stats ORDER BY app_name`)
	if err != nil {
		st.log.Warn("State: list app stats failed", "error", err)
		return nil
	}
	defer rows.Close()

	byApp := make(map[string]AppStats)
	for rows.Next() {
		var name string
		var data, updatedAt sql.NullString
		if err := rows.Scan(&name, &data, &updatedAt); err != nil {
			st.log.Warn("State: scan app stats failed", "error", err)
			return nil
		}
		fs, err := unmarshalAppStats(data)
		if err != nil {
			st.log.Warn("State: read app stats failed", "app", name, "error", err)
			continue
		}
		fs.UpdatedAt = updatedAt.String
		merged := byApp[name]
		mergeAppStatsInto(&merged, fs)
		merged.App = name
		byApp[name] = merged
	}
	if len(byApp) == 0 {
		return nil
	}
	out := make([]AppStats, 0, len(byApp))
	for _, fs := range byApp {
		out = append(out, fs)
	}
	sortAppStats(out)
	return out
}

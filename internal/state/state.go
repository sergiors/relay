package state

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	_ "modernc.org/sqlite"

	"relay/internal/function"
)

// DBPath is the fixed internal location of the local state database. It is an
// application convention, not env-configurable: the compose file volume-mounts
// /var/lib/relay so the file survives container restarts, and Open MkdirAll's
// the parent so host-side runs (and tests) work without it existing first.
const DBPath = "/var/lib/relay/db.sqlite3"

// Public persisted/CLI lifecycle statuses. The model is linear for a single
// generation — preparing -> building -> reconciling -> ready — with degraded
// and unavailable as terminal failure outcomes that respectively retain or lack
// a usable active generation. There is deliberately no "pending" status: a
// discovered function is "preparing" because both startup discovery/rebuild and
// a live desired-generation change reset it before the current work runs.
const (
	StatusPreparing   = "preparing"
	StatusBuilding    = "building"
	StatusReconciling = "reconciling"
	StatusReady       = "ready"
	StatusDegraded    = "degraded"
	StatusUnavailable = "unavailable"
)

// last reconcile status values for the persisted last_reconcile_status.
const (
	ReconcileSuccess = "success"
	ReconcileFailed  = "failed"
)

// Row is the per-function summary returned by ListFunctions. Its fields are part
// of the functions.data snapshot except the relational Name/UpdatedAt (columns)
// and the derived HandlerCount; it is embedded in Detail, so the JSON tags below
// also shape the persisted snapshot.
type Row struct {
	Name                string `json:"-"`
	Runtime             string `json:"runtime,omitempty"`
	Status              string `json:"status,omitempty"`
	HandlerCount        int    `json:"-"`
	LastReconcileStatus string `json:"last_reconcile_status,omitempty"`
	PreparedAt          string `json:"prepared_at,omitempty"`
	UpdatedAt           string `json:"-"`
}

// Detail is the full per-function record returned by GetFunction, including the
// handler list, the schedules, and the services. It is the persistence model:
// the whole snapshot is stored as one JSON object in functions.data (marshalled
// through function_json.go), with only name and updated_at kept as columns. The
// embedded Row fields and every field below — except Name, UpdatedAt, and the
// derived HandlerCount — are part of that snapshot.
type Detail struct {
	Row
	Image           string            `json:"image,omitempty"`
	Fingerprint     string            `json:"fingerprint,omitempty"`
	LastReconcileAt string            `json:"last_reconcile_at,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	Handlers        []Handler         `json:"handlers,omitempty"`
	Schedules       []Schedule        `json:"schedules,omitempty"`
	Services        []Service         `json:"services,omitempty"`
	Env             map[string]string `json:"env,omitempty"`
	Secrets         map[string]string `json:"secrets,omitempty"`
}

// Handler is one rule's handler, its resolved timeout, and its retry count (the
// same retry budget as schedules). The rule's pattern is deliberately NOT part
// of the persisted snapshot: matching is rebuilt from template.yaml, never from
// this read-only view, and the parser's opaque matcher interfaces cannot be
// JSON round-tripped.
type Handler struct {
	Name    string        `json:"name"`
	Timeout time.Duration `json:"timeout"`
	Retries int           `json:"retries"`
}

// Schedule is one cron schedule's handler, its verbatim cron expression, the
// effective timezone name (e.g. "UTC", "Europe/Rome"), its resolved
// per-invocation timeout, and its retry count (the same retry budget as event
// rules).
type Schedule struct {
	Handler  string        `json:"handler"`
	Cron     string        `json:"cron"`
	Timezone string        `json:"timezone"`
	Timeout  time.Duration `json:"timeout"`
	Retries  int           `json:"retries"`
}

// Service is one persistent service's effective configuration as persisted
// from the template: its source (exactly one of the entrypoint file or the
// external image reference), the optional routing host and path prefix, its
// internal TCP port, and the desired replica count. Its identity is the
// configured source descriptor (SourceRef), i.e. whichever of
// Entrypoint/Image is set; it is derived, never stored as a separate field.
type Service struct {
	Entrypoint string `json:"entrypoint,omitempty"`
	Image      string `json:"image,omitempty"`
	Host       string `json:"host,omitempty"`
	Path       string `json:"path,omitempty"`
	Port       int    `json:"port"`
	Replicas   int    `json:"replicas"`
}

// State is a concrete SQLite-backed local state view. It is safe for use from
// the reconciler goroutines; every operation uses a fresh connection context so
// no state is shared.
type State struct {
	db  *sql.DB
	log *slog.Logger
	// nowFn is an injectable clock used to stamp updated_at/reconcile
	// timestamps. Production leaves it nil and falls back to the package now()
	// (time.Now UTC RFC3339), preserving behavior exactly; tests inject a fake
	// to advance time deterministically without sleeping. Tests must set it
	// before any writes, so every stamped row uses the fake clock.
	nowFn func() time.Time
}

// fallbackLogger is the package-level default logger used when a State is
// opened without SetLogger (e.g. the CLI admin commands and the worker's
// early-open path before wiring). It runs at DEBUG so nothing is hidden; the
// worker replaces it with the process's leveled logger via SetLogger.
var fallbackLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

// Open creates (MkdirAll) the parent directory, opens (creating if absent) the
// database, applies concurrency-friendly PRAGMAs, and initializes the schema
// idempotently. It always returns a usable *State; schema errors surface via
// method calls, keeping the local state non-fatal to Relay at startup.
//
// Concurrency model: the pool is capped at one connection (SetMaxOpenConns(1)),
// so all in-process access is serialized by database/sql — callers queue on the
// single connection rather than ever hitting SQLITE_BUSY. WAL + busy_timeout
// cover cross-process access (a CLI process reading the same file while the
// worker runs). See doc.go "Concurrency model" for the full rationale.
func Open(path string) (*State, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open state %s: %w", path, err)
	}
	st := &State{db: db, log: fallbackLogger}

	// Serialize all access through a single pooled connection. Relay's local
	// state is a tiny, low-frequency, single-file workload (reconciler writes, a
	// 5s stats flush, CLI reads): a pool of 1 makes SQLITE_BUSY structurally
	// impossible — database/sql queues callers on the single connection instead
	// of letting SQLite reject concurrent writers. WAL+busy_timeout remain as
	// defense in depth (and for the CLI process opening the same file while the
	// worker runs: cross-PROCESS access still relies on them).
	db.SetMaxOpenConns(1)
	// With MaxOpenConns(1) the default MaxIdleConns(2) already exceeds the pool
	// size, so the single idle connection is retained automatically; no
	// SetMaxIdleConns needed. SetConnMaxLifetime/SetConnMaxIdleTime are skipped
	// too: there is no connection-rotation benefit for a single in-process
	// connection, and modernc sqlite has no server-side connection lifetime.

	// PRAGMAs tune SQLite for concurrent single-writer access: a busy timeout
	// bounds how long a writer waits for a lock, and WAL lets readers see the
	// last committed state while a write is in flight (relay's readers and the
	// single reconciler writer are distinct paths). Because MaxOpenConns(1) keeps
	// exactly one connection open for the pool's lifetime, these ExecContext
	// PRAGMAs run on that single connection and are effectively global for this
	// handle. busy_timeout is per-connection, so it still matters cross-process
	// (the CLI's own Open sets its own); journal_mode=WAL is persistent in the
	// DB file and survives close, so a concurrently-opening CLI also gets WAL.
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set busy_timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set WAL: %w", err)
	}
	if err := st.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

// SetLogger redirects log output; used by tests and CLI wiring. A nil logger (or a
// nil receiver) is a no-op, leaving the current logger in place.
func (st *State) SetLogger(logger *slog.Logger) {
	if st != nil && logger != nil {
		st.log = logger
	}
}

// Close releases the underlying connection pool. It is non-fatal on error.
func (st *State) Close() error { return st.db.Close() }

// initSchema creates the current tables if they do not already exist, so a
// fresh database is initialized with the current schema. Plain SQL: these
// tables are internal local state and are (re)created on first open.
//
// functions stores a whole per-function snapshot as one JSON object in data
// (SQLite's binary JSON/JSONB, written with jsonb(?) and read back with
// json(data)), with only the stable name key and the write timestamp kept as
// columns. There are deliberately no per-handler/per-schedule/per-service child
// tables: the nested configuration is part of the snapshot, so a template change
// replaces one row atomically. stats is the single-row global counter snapshot,
// and function_stats the per-function counterpart; both keep their stable key
// and updated_at relational while the evolving payload lives in data.
func (st *State) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS functions (
			name TEXT PRIMARY KEY,
			data BLOB NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		// stats holds the single "current operational snapshot" consumed by
		// Relay itself: monotonically increasing counters persisted across
		// restarts plus gauge snapshots of the pending backlog, never a
		// time-series. Only the stable relational metadata is a column — the
		// fixed single-row id and the write timestamp updated_at — while the
		// evolving counter/gauge payload is the JSON object in data (see Stats
		// and stats_json.go). JSON, not a column per field: the payload grows
		// as instrumentation is added without changing the schema, and absent
		// fields decode to zero.
		`CREATE TABLE IF NOT EXISTS stats (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			data BLOB NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		// function_stats holds the per-function operational snapshot, keyed by
		// function name. It mirrors the global stats table but attributes each
		// payload to a single function (see FunctionStats for the semantics).
		// The key function_name and the write timestamp updated_at stay
		// relational; the payload — including the cumulative warm-container pool
		// counters (warm_acquires_total, cold_starts_total, discarded_total) and
		// the four last_*_at execution-history timestamps — lives in the JSONB
		// data column (see stats_json.go). Backlog metrics
		// (pending_entries/oldest_pending_age) stay global-only in stats: they
		// describe the stream backlog, not any one function. The live pool
		// gauges are deliberately never persisted.
		`CREATE TABLE IF NOT EXISTS function_stats (
			function_name TEXT PRIMARY KEY,
			data BLOB NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	}

	for _, s := range stmts {
		if _, err := st.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}

	return nil
}

// now returns the current UTC time in RFC3339. It is the package-level default
// clock; State.nowString prefers the injectable clock when one is set.
func now() string { return time.Now().UTC().Format(time.RFC3339) }

// nowString returns the current time as an RFC3339 UTC string, using the
// injectable clock when set and the package default otherwise. It is the single
// timestamp source for every State write, so an injected clock (tests) governs
// every updated_at/last_reconcile_at consistently.
func (st *State) nowString() string {
	if st.nowFn != nil {
		return st.nowFn().UTC().Format(time.RFC3339)
	}
	return now()
}

// rebuildTx runs fn inside a transaction, which the rebuild path uses so a
// partial scan never leaves a half-populated state database.
func (st *State) rebuildTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// empty reports whether the functions table has no rows. Only an empty state
// database is rebuilt from disk — we never overwrite existing state from
// /functions.
func (st *State) empty(ctx context.Context) (bool, error) {
	var n int
	err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM functions`).Scan(&n)
	return n == 0, err
}

// DiscoveredFunction pairs a function loaded by the caller with the fingerprint
// the caller computed for it. It is the input to the worker's startup state
// phase: the worker hashes each loaded function once and reuses that value for
// both the fresh-database rebuild and the per-function discovery upsert, so no
// state write re-reads the function's source tree.
type DiscoveredFunction struct {
	Function    function.Function
	Fingerprint string
}

// RebuildFromFS populates an empty state database by scanning dir with the real
// function loader and per-function fingerprinting. Newly loaded functions are
// recorded as status=preparing (loaded, not yet built/verified). It is a no-op
// when the state database already has rows — /functions is the source of truth,
// but only for (re)seeding a fresh database. It remains the standalone entry
// point for callers that have not already loaded the functions (tests and other
// standalone callers); the worker uses RebuildFromFunctions so its already-loaded
// set is not re-read from disk.
func (st *State) RebuildFromFS(dir string) error {
	ctx := context.Background()
	populated, err := st.empty(ctx)
	if err != nil {
		return fmt.Errorf("check state empty: %w", err)
	}
	if !populated {
		return nil
	}

	loader := function.NewLoader(dir, st.log)
	fns, err := loader.Load()
	if err != nil {
		return fmt.Errorf("load functions for state: %w", err)
	}
	return st.rebuildDiscoveredTx(ctx, st.fingerprintFunctions(fns))
}

// RebuildFromFunctions populates an empty state database from functions the
// caller already loaded, reusing the caller's per-function fingerprint instead
// of loading the directory again. It is the worker's startup path: the same
// loaded set and fingerprints feed this seed and the subsequent
// RecordDiscoveredWithFingerprint upserts, so each function is hashed exactly
// once per state phase. Like RebuildFromFS it is a no-op when the database
// already has rows.
func (st *State) RebuildFromFunctions(discovered []DiscoveredFunction) error {
	ctx := context.Background()
	populated, err := st.empty(ctx)
	if err != nil {
		return fmt.Errorf("check state empty: %w", err)
	}
	if !populated {
		return nil
	}
	return st.rebuildDiscoveredTx(ctx, discovered)
}

// rebuildDiscoveredTx writes every prepared discovery in one transaction so a
// partial scan never leaves a half-populated database. Fingerprints are computed
// (or supplied) BEFORE this call: the transaction must hold no external I/O
// (filesystem reads) while it is open, so the body only writes.
func (st *State) rebuildDiscoveredTx(ctx context.Context, discovered []DiscoveredFunction) error {
	return st.rebuildTx(ctx, func(tx *sql.Tx) error {
		for _, d := range discovered {
			detail := functionSnapshot(d.Function.Name, d.Function.Template, StatusPreparing, "", d.Fingerprint, "", "", "", "")
			detail.UpdatedAt = st.nowString()
			if err := upsertFunctionTx(ctx, tx, detail); err != nil {
				return err
			}
		}
		return nil
	})
}

// fingerprintFunctions computes each loaded function's fingerprint once, logging
// and falling back to fp="" on error exactly as the discovery path always has.
func (st *State) fingerprintFunctions(fns []function.Function) []DiscoveredFunction {
	discovered := make([]DiscoveredFunction, 0, len(fns))
	for _, fn := range fns {
		discovered = append(discovered, DiscoveredFunction{Function: fn, Fingerprint: st.fingerprint(fn)})
	}
	return discovered
}

// fingerprint computes one function's content fingerprint, logging a warning and
// returning "" on error. The caller computes it BEFORE the write transaction, so
// the tx closure only writes; every state discovery path shares this fallback. It
// uses the same narrowest-input rule as the reconciler and worker
// (function.FingerprintFunction), so a no-runtime function is fingerprinted over
// template.yaml alone.
func (st *State) fingerprint(fn function.Function) string {
	fp, err := function.FingerprintFunction(fn.Dir, fn.Template)
	if err != nil {
		st.log.Warn("State: fingerprint failed", "function", fn.Name, "error", err)
		return ""
	}
	return fp
}

// RecordDiscovered records a function discovered from /functions on a fresh
// state database (or when no row exists). It sets runtime/status=preparing, the
// fingerprint, and the full configuration snapshot, clearing any stale prior
// state. It is an upsert keyed by name. Startup discovery (and the RebuildFromFS
// seed) runs it for every loaded function BEFORE any current-generation work, so
// a stale ready/building/reconciling value left by a process that died mid-work
// is reset to preparing rather than persisting forever. It computes the
// fingerprint itself; callers that already hold one use
// RecordDiscoveredWithFingerprint.
func (st *State) RecordDiscovered(fn function.Function) {
	st.RecordDiscoveredWithFingerprint(fn, st.fingerprint(fn))
}

// RecordDiscoveredWithFingerprint records a discovered function using a
// fingerprint supplied by the caller, so a caller that already computed it (the
// worker's startup state phase) never re-reads the function's source. The write
// transaction holds no external I/O: the fingerprint is passed in, not computed
// inside the closure.
func (st *State) RecordDiscoveredWithFingerprint(fn function.Function, fingerprint string) {
	ctx := context.Background()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail := functionSnapshot(fn.Name, fn.Template, StatusPreparing, "", fingerprint, "", "", "", "")
		detail.UpdatedAt = st.nowString()
		return upsertFunctionTx(ctx, tx, detail)
	})
	if err != nil {
		st.log.Warn("State: record discovered failed", "function", fn.Name, "error", err)
	}
}

// RecordReconcileSuccess records that a function built and serves an active
// version: status=ready, the new image/fingerprint/prepared_at,
// last_reconcile_status=success AND last_reconcile_at=now (the last meaningful
// reconcile), cleared last_error, and the full configuration snapshot. On
// conflict (existing row) the upsert replaces the whole snapshot, so a success
// on a previously-discovered row records its own outcome and timestamp.
func (st *State) RecordReconcileSuccess(
	name,
	image,
	fingerprint string,
	preparedAt time.Time,
	fn function.Function,
) {
	ctx := context.Background()
	ts := st.nowString()
	prepared := preparedAt.UTC().Format(time.RFC3339)
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail := functionSnapshot(name, fn.Template, StatusReady, image, fingerprint, prepared, ts, ReconcileSuccess, "")
		detail.UpdatedAt = ts
		return upsertFunctionTx(ctx, tx, detail)
	})
	if err != nil {
		st.log.Warn("State: record success failed", "function", name, "error", err)
	}
}

// RecordPreparing marks the desired configuration as in progress while retaining
// the last active generation. It is written when a live desired-generation change
// is detected, before any current-generation work runs, so the public status
// reflects that the function is preparing a new generation. The active
// image/fingerprint/prepared_at are only replaced by a successful full
// reconcile, so a later failure never hides a healthy version.
func (st *State) RecordPreparing(name string, fn function.Function) {
	ctx := context.Background()
	ts := st.nowString()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanFunction(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at FROM functions WHERE name = ?`, name))
		if err != nil {
			return err
		}
		if !found {
			detail = functionSnapshot(name, fn.Template, StatusPreparing, "", "", "", "", "", "")
		} else {
			active := detail
			detail = functionSnapshot(name, fn.Template, StatusPreparing, active.Image, active.Fingerprint, active.PreparedAt, active.LastReconcileAt, active.LastReconcileStatus, active.LastError)
		}
		detail.UpdatedAt = ts
		return upsertFunctionTx(ctx, tx, detail)
	})
	if err != nil {
		st.log.Warn("State: record preparing failed", "function", name, "error", err)
	}
}

// RecordReconcileBuilding changes only the lifecycle status to building. It is
// called at the actual managed-runtime image build boundary (a function or
// dependency image build), not when a build is merely queued. Persistent
// services have no separate image build, so the service path never publishes
// building.
func (st *State) RecordReconcileBuilding(name string) {
	st.recordStatus(name, StatusBuilding)
}

// RecordReconciling changes only the lifecycle status to reconciling. It is
// called at the actual convergence seam of a pass that has corrective container
// work to perform (entrypoint/image services), so the status never claims
// convergence work that has not started. A fully-converged no-op verification
// pass never calls it: an unchanged function stays ready rather than flashing
// reconciling.
func (st *State) RecordReconciling(name string) {
	st.recordStatus(name, StatusReconciling)
}

func (st *State) recordStatus(name, status string) {
	ctx := context.Background()
	ts := st.nowString()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanFunction(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at FROM functions WHERE name = ?`, name))
		if err != nil || !found {
			return err
		}
		detail.Status, detail.UpdatedAt = status, ts
		payload, err := marshalFunction(detail)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE functions SET data = jsonb(?), updated_at = ? WHERE name = ?`, payload, ts, name)
		return err
	})
	if err != nil {
		st.log.Warn("State: record status failed", "function", name, "status", status, "error", err)
	}
}

// RecordReconcileFailure records that a reconcile build failed.
//
// Key state model: the image/fingerprint/prepared_at of the PREVIOUS active
// version are deliberately left intact so the last good build still serves;
// only last_reconcile_at/last_reconcile_status (failed) and last_error/updated_at
// change. The rest of the persisted snapshot is preserved by a read-modify-write
// inside the transaction. Status is ready when an active image remains and
// unavailable otherwise.
func (st *State) RecordReconcileFailure(name string, err2 error) {
	ctx := context.Background()
	ts := st.nowString()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanFunction(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM functions WHERE name = ?`, name))
		if err != nil {
			return err
		}
		if !found {
			// The old UPDATE ... WHERE name = ? was a no-op on an absent row.
			return nil
		}
		detail.LastReconcileAt = ts
		detail.LastReconcileStatus = ReconcileFailed
		detail.LastError = err2.Error()
		// A failed attempt never displaces the last healthy generation.
		detail.Status = StatusUnavailable
		if detail.Image != "" {
			detail.Status = StatusReady
		}
		detail.UpdatedAt = ts
		payload, err := marshalFunction(detail)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`UPDATE functions SET data = jsonb(?), updated_at = ? WHERE name = ?`,
			payload, ts, name)
		return err
	})
	if err != nil {
		st.log.Warn("State: record failure failed", "function", name, "error", err)
	}
}

// RecordServiceFailure records a service convergence failure without hiding a
// healthy function image. A function with an active image is degraded; one
// without an active image is unavailable.
func (st *State) RecordServiceFailure(name string, err2 error) {
	ctx := context.Background()
	ts := st.nowString()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanFunction(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM functions WHERE name = ?`, name))
		if err != nil || !found {
			return err
		}
		detail.LastReconcileAt = ts
		detail.LastReconcileStatus = ReconcileFailed
		detail.LastError = err2.Error()
		if detail.Image != "" {
			detail.Status = StatusDegraded
		} else {
			detail.Status = StatusUnavailable
		}
		detail.UpdatedAt = ts
		payload, err := marshalFunction(detail)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`UPDATE functions SET data = jsonb(?), updated_at = ? WHERE name = ?`,
			payload, ts, name)
		return err
	})
	if err != nil {
		st.log.Warn("State: record service failure failed", "function", name, "error", err)
	}
}

// RecordRemoved deletes a function and its per-function stats from the state
// database, so a removed function never leaves a stale stats row behind.
func (st *State) RecordRemoved(name string) {
	ctx := context.Background()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		return removeTx(ctx, tx, name)
	})
	if err != nil {
		st.log.Warn("State: record removed failed", "function", name, "error", err)
	}
}

// PruneRemoved removes state rows for every function recorded in the database
// that no longer exists in dir (the authoritative /functions root), including
// its function_stats. The filesystem is the source of truth; this only removes
// rows for functions genuinely absent from disk. Transient stat errors
// (permissions/I/O) are skipped — a flaky read must not drop a function that is
// still on disk, mirroring the reconciler's removal tolerance. The global
// single-row stats table is deliberately untouched. It is intended to run at
// startup, before the fresh registry's counters are seeded from the persisted
// function_stats (restorePersistedStats), so a function removed while the worker
// was down is pruned before its stale function_stats row could be re-seeded into
// metrics.
func (st *State) PruneRemoved(dir string) {
	ctx := context.Background()

	// Collect every recorded name up front and close the rows before deleting:
	// the delete transaction below acquires its own connection from the pool, and
	// fully consuming the query first keeps the read and write paths independent.
	rows, err := st.db.QueryContext(ctx, `SELECT name FROM functions ORDER BY name`)
	if err != nil {
		st.log.Warn("State: prune removed: list functions failed", "error", err)
		return
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			st.log.Warn("State: prune removed: scan name failed", "error", err)
			_ = rows.Close()
			return
		}
		names = append(names, name)
	}
	_ = rows.Close()

	for _, name := range names {
		if _, err := os.Stat(filepath.Join(dir, name)); os.IsNotExist(err) {
			// Reuse the same single-function removal path as RecordRemoved so the
			// live reconciler and the startup sweep behave identically.
			if rerr := st.rebuildTx(ctx, func(tx *sql.Tx) error {
				return removeTx(ctx, tx, name)
			}); rerr != nil {
				st.log.Warn("State: prune removed failed", "function", name, "error", rerr)
				continue
			}
			st.log.Info("State: function pruned at startup", "function", name)
		}
		// Any other stat error (permissions/I/O) is skipped: only a genuine
		// os.IsNotExist means the function was removed from the filesystem.
	}
}

// removeTx deletes a function and its per-function stats row — the functions
// row itself and the matching function_stats row — inside tx. The nested
// handler/schedule/service configuration lives inside functions.data, so no
// child-table delete is needed. It is shared by the live reconciler removal
// (RecordRemoved) and the startup sweep (PruneRemoved) so both are behaviorally
// identical: a removed function never leaves a stale function_stats row behind.
func removeTx(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM function_stats WHERE function_name = ?`, name); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM functions WHERE name = ?`, name)
	return err
}

// ListFunctions returns the function summaries sorted by name, decoding the
// relational name/updated_at columns and the functions.data snapshot for each
// row. A nil/empty slice and nil error mean the state database is simply empty.
// A row whose stored snapshot is corrupt is logged (naming the function and the
// decode error) and skipped, so one bad row cannot hide the others.
func (st *State) ListFunctions() []Row {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx,
		`SELECT name, `+jsonPayloadExpr+`, updated_at
		 FROM functions ORDER BY name`)
	if err != nil {
		st.log.Warn("State: list functions failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var (
			name      string
			data      sql.NullString
			updatedAt string
		)
		if err := rows.Scan(&name, &data, &updatedAt); err != nil {
			st.log.Warn("State: scan list failed", "error", err)
			return out
		}
		detail, err := unmarshalFunction(data.String)
		if err != nil {
			st.log.Warn("State: read function payload failed", "function", name, "error", err)
			continue
		}
		detail.Name = name
		detail.UpdatedAt = updatedAt
		detail.HandlerCount = len(detail.Handlers)
		out = append(out, detail.Row)
	}
	return out
}

// GetFunction returns the full detail for name, or (zero, false) if unknown. A
// corrupt persisted snapshot fails clearly: the decode error is logged and the
// row reads as unknown rather than yielding a partial record.
func (st *State) GetFunction(name string) (Detail, bool) {
	ctx := context.Background()
	detail, found, err := scanFunction(name, st.db.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+`, updated_at
		 FROM functions WHERE name = ?`, name))
	if err != nil {
		st.log.Warn("State: get failed", "function", name, "error", err)
		return Detail{}, false
	}
	if !found {
		return Detail{}, false
	}
	return detail, true
}

// scanFunction decodes one functions row selected as (json(data), updated_at)
// into a Detail. name is relational metadata supplied by the caller (it is not
// part of the payload). found is false for an absent row; a stored payload whose
// JSON is invalid is returned as an error so the caller can log a clear decode
// failure.
func scanFunction(name string, row *sql.Row) (Detail, bool, error) {
	var data sql.NullString
	var updatedAt sql.NullString
	if err := row.Scan(&data, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return Detail{}, false, nil
		}
		return Detail{}, false, err
	}
	detail, err := unmarshalFunction(data.String)
	if err != nil {
		return Detail{}, false, err
	}
	detail.Name = name
	detail.UpdatedAt = updatedAt.String
	detail.HandlerCount = len(detail.Handlers)
	return detail, true, nil
}

// upsertFunctionTx writes the whole Detail snapshot as one JSONB value in
// functions.data, upserting on the name key. The snapshot replaces the previous
// one atomically, so a template change never leaves a mixture of old and new
// handler/schedule/service configuration.
func upsertFunctionTx(ctx context.Context, tx *sql.Tx, detail Detail) error {
	payload, err := marshalFunction(detail)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO functions (name, data, updated_at) VALUES (?, jsonb(?), ?)
		 ON CONFLICT(name) DO UPDATE SET
		   data       = excluded.data,
		   updated_at = excluded.updated_at`,
		detail.Name, payload, detail.UpdatedAt)
	return err
}

// functionSnapshot builds the persisted snapshot for a function template: the
// lifecycle fields passed in PLUS the whole configuration (env/secret
// references, handlers, schedules, services). Name is relational
// metadata and UpdatedAt is stamped by the caller; HandlerCount is derived on
// read. Secret entries hold only the reference name — never a resolved value.
func functionSnapshot(
	name string,
	tmpl *function.Template,
	status,
	image,
	fingerprint,
	prepared,
	reconcileAt,
	reconcileStatus,
	lastError string,
) Detail {
	env, secrets := snapshotConfig(tmpl)
	return Detail{
		Row: Row{
			Name:                name,
			Runtime:             tmpl.Runtime,
			Status:              status,
			LastReconcileStatus: reconcileStatus,
			PreparedAt:          prepared,
		},
		Image:           image,
		Fingerprint:     fingerprint,
		LastReconcileAt: reconcileAt,
		LastError:       lastError,
		Handlers:        snapshotHandlers(tmpl),
		Schedules:       snapshotSchedules(tmpl),
		Services:        snapshotServices(tmpl),
		Env:             env,
		Secrets:         secrets,
	}
}

// snapshotConfig copies a template's env and secret MAPPINGS for the persisted
// snapshot. Only the env/secret MAPPINGS are
// stored (env-var name → literal value, and env-var name → secret reference) —
// never a secret VALUE. Empty maps stay nil so the payload omits them and
// a read-back yields nil (the CLI's "section absent" convention).
func snapshotConfig(tmpl *function.Template) (env, secrets map[string]string) {
	if len(tmpl.Env) > 0 {
		env = make(map[string]string, len(tmpl.Env))
		for name, value := range tmpl.Env {
			env[name] = value
		}
	}
	if len(tmpl.Secrets) > 0 {
		secrets = make(map[string]string, len(tmpl.Secrets))
		for name, ref := range tmpl.Secrets {
			secrets[name] = ref.String()
		}
	}
	return env, secrets
}

// snapshotHandlers renders the template's event rules as name-ordered handlers
// (name + resolved timeout). It mirrors the ordering the former handlers table
// produced (ORDER BY handler), so list/detail output is unchanged.
func snapshotHandlers(tmpl *function.Template) []Handler {
	if len(tmpl.Events) == 0 {
		return nil
	}
	out := make([]Handler, 0, len(tmpl.Events))
	for _, rule := range tmpl.Events {
		out = append(out, Handler{Name: rule.Handler, Timeout: rule.Timeout, Retries: rule.Retries})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// snapshotSchedules renders the template's cron schedules with their verbatim
// expression, effective location name, resolved timeout, and retry count. It
// mirrors the former schedules table ordering (ORDER BY handler, cron).
func snapshotSchedules(tmpl *function.Template) []Schedule {
	if len(tmpl.Schedules) == 0 {
		return nil
	}
	out := make([]Schedule, 0, len(tmpl.Schedules))
	for _, s := range tmpl.Schedules {
		// time.Location.String() is nil-safe (a nil location reports "UTC"),
		// matching the persisted timezone semantics of the former table.
		out = append(out, Schedule{
			Handler:  s.Handler,
			Cron:     s.Cron,
			Timezone: s.Location.String(),
			Timeout:  s.Timeout,
			Retries:  s.Retries,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Handler != out[j].Handler {
			return out[i].Handler < out[j].Handler
		}
		return out[i].Cron < out[j].Cron
	})
	return out
}

// snapshotServices renders the template's persistent services with their source
// (exactly one of entrypoint/image), routing host/path, effective port, and
// desired replicas. It mirrors the former services table ordering (ORDER BY
// entrypoint, image), so a source-kind service keeps a deterministic position.
func snapshotServices(tmpl *function.Template) []Service {
	if len(tmpl.Services) == 0 {
		return nil
	}
	out := make([]Service, 0, len(tmpl.Services))
	for _, s := range tmpl.Services {
		out = append(out, Service{
			Entrypoint: s.Entrypoint,
			Image:      s.Image,
			Host:       s.Host,
			Path:       s.Path,
			Port:       s.Port,
			Replicas:   s.Replicas,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Entrypoint != out[j].Entrypoint {
			return out[i].Entrypoint < out[j].Entrypoint
		}
		return out[i].Image < out[j].Image
	})
	return out
}

// RelativeAgo renders an RFC3339 timestamp as a short relative age: "12s ago",
// "3m ago", "2h ago", "5d ago". Beyond ~30 days it falls back to an absolute
// date (YYYY-MM-DD) because a coarse relative age is no longer informative.
// It is shared between the state tests and the read-only CLI.
func RelativeAgo(rfc3339 string) string {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		return rfc3339
	}
	now := time.Now()
	if t.After(now) {
		return "just now"
	}
	age := now.Sub(t)
	switch {
	case age < time.Minute:
		return fmt.Sprintf("%ds ago", int(age.Seconds()))
	case age < time.Hour:
		return fmt.Sprintf("%dm ago", int(age.Minutes()))
	case age < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(age.Hours()))
	case age < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(age.Hours()/24))
	default:
		return t.Format("2006-01-02")
	}
}

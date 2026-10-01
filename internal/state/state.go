package state

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"relay/internal/app"
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
// discovered app is "preparing" because both startup discovery/rebuild and
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

// Row is the per-app summary returned by ListApps. Its fields are part
// of the apps.data snapshot except the relational Name/UpdatedAt (columns)
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

// Detail is the full per-app record returned by GetApp, including the
// handler list, the schedules, and the services. It is the persistence model:
// the whole snapshot is stored as one JSON object in apps.data (marshalled
// through app_json.go), with only name and updated_at kept as columns. The
// embedded Row fields and every field below — except Name, UpdatedAt, and the
// derived HandlerCount — are part of that snapshot.
//
// Env holds env-var names only (each value is RedactedEnvValue); literal env
// values are never persisted. Secrets holds secret reference names only, never
// resolved values.
//
// Generation model: Image/Fingerprint/PreparedAt describe the last USABLE
// (successfully prepared and serving) generation and are only replaced by a
// success. DesiredFingerprint is the latest desired content fingerprint seen by
// discovery or a live desired-generation change; it may differ from Fingerprint
// while a new generation is being prepared, and a reconcile FAILURE of a valid
// desired template leaves both the active generation and the desired fingerprint
// in place (the failed generation is still the desired one, so a later pass can
// retry it). An INVALID desired definition is different: it cannot be loaded at
// all, so there is no trustworthy desired fingerprint or configuration to keep,
// and RecordInvalidDesired clears both while preserving the active generation.
// LastReconcileAt/Status and LastError describe the last MEANINGFUL reconcile
// outcome.
type Detail struct {
	Row
	Image              string     `json:"image,omitempty"`
	Fingerprint        string     `json:"fingerprint,omitempty"`
	DesiredFingerprint string     `json:"desired_fingerprint,omitempty"`
	LastReconcileAt    string     `json:"last_reconcile_at,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
	Handlers           []Handler  `json:"handlers,omitempty"`
	Schedules          []Schedule `json:"schedules,omitempty"`
	Services           []Service  `json:"services,omitempty"`
	// Env holds the app's env-var NAMES only: each value is the fixed
	// RedactedEnvValue marker, never the literal template value. Literal env
	// values live only in template.yaml and in the runtime injection path; the
	// state snapshot (a local file an operator can read) must never carry them.
	Env     map[string]string `json:"env,omitempty"`
	Secrets map[string]string `json:"secrets,omitempty"`
	// Resources is the app's EFFECTIVE per-container resource configuration
	// (memory/cpus/pids), resolved with defaults. It is part of the persisted
	// snapshot so `relay app inspect` can render it; the values are
	// configuration (never secrets), so storing them adds no sensitivity.
	Resources *Resources `json:"resources,omitempty"`
}

// Resources is an app's effective per-container resource configuration as
// persisted from the template: memory in bytes, cpus as a floating-point core
// count, and the PID limit. It is a derived, read-only view of the template; the
// parser's exact byte/nano-CPU representation is preserved so no precision is
// lost, and the CLI renders a human-readable form.
type Resources struct {
	MemoryBytes int64   `json:"memory_bytes"`
	CPUs        float64 `json:"cpus"`
	Pids        int64   `json:"pids"`
}

// hasUsableGeneration reports whether the last active generation is usable, i.e.
// a successful reconcile produced something that can still serve. A
// runtime-backed app signals this with a non-empty app Image. A
// no-runtime, external-image service-only app builds no app image at
// all, so its successful generation is signalled instead by a non-empty
// PreparedAt (stamped only by RecordReconcileSuccess), even though its Image is
// empty. Either marker means a failure is degraded (the prior generation is
// retained) rather than unavailable (nothing was ever successfully prepared).
func (d Detail) hasUsableGeneration() bool {
	return d.Image != "" || d.PreparedAt != ""
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

// Schedule is one cron schedule's stable name, the handler it invokes, its
// verbatim cron expression, the effective timezone name (e.g. "UTC",
// "Europe/Rome"), its resolved per-invocation timeout, and its retry count (the
// same retry budget as event rules). Name is the schedule's identity.
type Schedule struct {
	Name     string        `json:"name"`
	Handler  string        `json:"handler"`
	Cron     string        `json:"cron"`
	Timezone string        `json:"timezone"`
	Timeout  time.Duration `json:"timeout"`
	Retries  int           `json:"retries"`
}

// Service is one persistent service's stable name plus its effective
// configuration as persisted from the template: its source (exactly one of the
// entrypoint file or the external image reference), the optional routing host
// and path prefix, its internal TCP port, and the desired replica count. Name is
// the service's identity; the source is its implementation, not its identity.
type Service struct {
	Name       string `json:"name"`
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

	// statusObserverMu guards statusObserver. Status writes happen from several
	// goroutines (the reconciler pump, the service coordinator, startup
	// preparation), so the observer is read under RLock; it is installed once by
	// the worker right after Open and normally never changes.
	statusObserverMu sync.RWMutex
	// statusObserver, when non-nil, is notified AFTER each successful status
	// write with the app name and its new public status. The empty status
	// signals that the app was removed/pruned and its status series should
	// be deleted. It exists so observability (the metrics registry) can keep the
	// one-hot app_status gauge in sync with state without this package
	// importing metrics. A nil observer (or state opened without wiring) is a
	// silent no-op; state never depends on it.
	statusObserver func(name, status string)
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

// SetStatusObserver installs the observer notified after each SUCCESSFUL status
// write, called with the app name and its new public status. The metrics
// wiring uses it to keep the one-hot relay_app_status gauge in sync with
// the persisted lifecycle without the state package importing metrics. A nil
// observer (or a nil receiver) clears/leaves the observer unset and is safe.
//
// The observer is invoked with the empty status when an app is removed or
// pruned, which is the signal to delete the app's status series. It is
// called AFTER the write transaction commits, so an observer can never observe
// a status the database did not persist, and it is never called for a failed
// write (the gauge then keeps its prior value rather than claiming a transition
// that did not land).
func (st *State) SetStatusObserver(observer func(name, status string)) {
	if st == nil {
		return
	}
	st.statusObserverMu.Lock()
	st.statusObserver = observer
	st.statusObserverMu.Unlock()
}

// notifyStatus calls the installed status observer, if any. It is a no-op when
// no observer is installed or the receiver is nil.
func (st *State) notifyStatus(name, status string) {
	if st == nil {
		return
	}
	st.statusObserverMu.RLock()
	observer := st.statusObserver
	st.statusObserverMu.RUnlock()
	if observer != nil {
		observer(name, status)
	}
}

// Close releases the underlying connection pool. It is non-fatal on error.
func (st *State) Close() error { return st.db.Close() }

// initSchema creates the current tables if they do not already exist, so a
// fresh database is initialized with the current schema. Plain SQL: these
// tables are internal local state and are (re)created on first open.
//
// apps stores a whole per-app snapshot as one JSON object in data
// (SQLite's binary JSON/JSONB, written with jsonb(?) and read back with
// json(data)), with only the stable name key and the write timestamp kept as
// columns. There are deliberately no per-handler/per-schedule/per-service child
// tables: the nested configuration is part of the snapshot, so a template change
// replaces one row atomically. stats is the single-row global counter snapshot,
// and app_stats the per-app counterpart; both keep their stable key
// and updated_at relational while the evolving payload lives in data.
func (st *State) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS apps (
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
		// app_stats holds the per-app operational snapshot, keyed by
		// app name. It mirrors the global stats table but attributes each
		// payload to a single app (see AppStats for the semantics).
		// The key app_name and the write timestamp updated_at stay
		// relational; the payload — including the cumulative warm-container pool
		// counters (warm_acquires_total, cold_starts_total, discarded_total) and
		// the four last_*_at execution-history timestamps — lives in the JSONB
		// data column (see stats_json.go). Backlog metrics
		// (pending_entries/oldest_pending_age) stay global-only in stats: they
		// describe the stream backlog, not any one app. The live pool
		// gauges are deliberately never persisted.
		`CREATE TABLE IF NOT EXISTS app_stats (
			app_name TEXT PRIMARY KEY,
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

// empty reports whether the apps table has no rows. Only an empty state
// database is rebuilt from disk — we never overwrite existing state from
// /apps.
func (st *State) empty(ctx context.Context) (bool, error) {
	var n int
	err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM apps`).Scan(&n)
	return n == 0, err
}

// DiscoveredApp pairs an app loaded by the caller with the fingerprint
// the caller computed for it. It is the input to the worker's startup state
// phase: the worker hashes each loaded app once and reuses that value for
// both the fresh-database rebuild and the per-app discovery upsert, so no
// state write re-reads the app's source tree.
type DiscoveredApp struct {
	App         app.App
	Fingerprint string
}

// RebuildFromFS populates an empty state database by scanning dir with the real
// app loader and per-app fingerprinting. Newly loaded apps are
// recorded as status=preparing (loaded, not yet built/verified). It is a no-op
// when the state database already has rows — /apps is the source of truth,
// but only for (re)seeding a fresh database. It remains the standalone entry
// point for callers that have not already loaded the apps (tests and other
// standalone callers); the worker uses RebuildFromApps so its already-loaded
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

	loader := app.NewLoader(dir, st.log)
	fns, err := loader.Load()
	if err != nil {
		return fmt.Errorf("load apps for state: %w", err)
	}
	return st.rebuildDiscoveredTx(ctx, st.fingerprintApps(fns))
}

// RebuildFromApps populates an empty state database from apps the
// caller already loaded, reusing the caller's per-app fingerprint instead
// of loading the directory again. It is the worker's startup path: the same
// loaded set and fingerprints feed this seed and the subsequent
// RecordDiscoveredWithFingerprint upserts, so each app is hashed exactly
// once per state phase. Like RebuildFromFS it is a no-op when the database
// already has rows.
func (st *State) RebuildFromApps(discovered []DiscoveredApp) error {
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
// (filesystem reads) while it is open, so the body only writes. On success every
// seeded app's preparing status is published to the status observer.
func (st *State) rebuildDiscoveredTx(ctx context.Context, discovered []DiscoveredApp) error {
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		for _, d := range discovered {
			detail := appSnapshot(d.App.Name, d.App.Template, StatusPreparing, "", "", d.Fingerprint, "", "", "", "")
			detail.UpdatedAt = st.nowString()
			if err := upsertAppTx(ctx, tx, detail); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, d := range discovered {
		st.notifyStatus(d.App.Name, StatusPreparing)
	}
	return nil
}

// fingerprintApps computes each loaded app's fingerprint once, logging
// and falling back to fp="" on error exactly as the discovery path always has.
func (st *State) fingerprintApps(fns []app.App) []DiscoveredApp {
	discovered := make([]DiscoveredApp, 0, len(fns))
	for _, fn := range fns {
		discovered = append(discovered, DiscoveredApp{App: fn, Fingerprint: st.fingerprint(fn)})
	}
	return discovered
}

// fingerprint computes one app's content fingerprint, logging a warning and
// returning "" on error. The caller computes it BEFORE the write transaction, so
// the tx closure only writes; every state discovery path shares this fallback. It
// uses the same narrowest-input rule as the reconciler and worker
// (app.FingerprintApp), so a no-runtime app is fingerprinted over
// template.yaml alone.
func (st *State) fingerprint(fn app.App) string {
	fp, err := app.FingerprintApp(fn.Dir, fn.Template)
	if err != nil {
		st.log.Warn("State: fingerprint failed", "app", fn.Name, "error", err)
		return ""
	}
	return fp
}

// RecordDiscovered records an app discovered from /apps on a fresh
// state database (or when no row exists). It sets runtime/status=preparing, the
// desired fingerprint, and the full configuration snapshot, while PRESERVING the
// last usable active generation and the previous last-reconcile outcome. It is an
// upsert keyed by name. Startup discovery (and the RebuildFromFS seed) runs it
// for every loaded app BEFORE any current-generation work, so a transient
// ready/building/reconciling lifecycle value left by a process that died mid-work
// is reset to preparing rather than persisting forever, without erasing a
// still-serving image or a recent failure outcome. It computes the fingerprint
// itself; callers that already hold one use RecordDiscoveredWithFingerprint.
func (st *State) RecordDiscovered(fn app.App) {
	st.RecordDiscoveredWithFingerprint(fn, st.fingerprint(fn))
}

// RecordDiscoveredWithFingerprint records a discovered app using a
// fingerprint supplied by the caller, so a caller that already computed it (the
// worker's startup state phase) never re-reads the app's source.
//
// Discovery is a desired-generation event, not a reconcile: it refreshes the
// configuration snapshot and DesiredFingerprint and resets the public status to
// preparing, but it PRESERVES the last usable active generation
// (image/fingerprint/prepared_at) and the previous last-reconcile outcome.
// Startup rediscovery must not erase a still-serving image (the startup image
// sweep keeps it) nor hide a recent failure behind an empty outcome. On a
// app with no prior row the active generation is empty and only the desired
// fingerprint is recorded.
//
// The write transaction holds no external I/O: the fingerprint is passed in, not
// computed inside the closure.
func (st *State) RecordDiscoveredWithFingerprint(fn app.App, fingerprint string) {
	ctx := context.Background()
	ts := st.nowString()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		return recordDesiredTx(ctx, tx, fn.Name, fn.Template, fingerprint, ts)
	})
	if err != nil {
		st.log.Warn("State: record discovered failed", "app", fn.Name, "error", err)
		return
	}
	st.notifyStatus(fn.Name, StatusPreparing)
}

// recordDesiredTx is the shared body of the desired-generation writes (startup
// discovery and live desired-change detection): it refreshes the whole
// configuration snapshot and the desired fingerprint and sets status=preparing
// while preserving the last usable active generation
// (image/fingerprint/prepared_at) and the previous last-reconcile outcome. Only
// a successful full reconcile replaces the active generation. An absent row is
// created with no active generation and the supplied desired fingerprint.
func recordDesiredTx(
	ctx context.Context,
	tx *sql.Tx,
	name string,
	tmpl *app.Template,
	fingerprint,
	ts string,
) error {
	detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+`, updated_at FROM apps WHERE name = ?`, name))
	if err != nil {
		return err
	}
	if !found {
		detail = appSnapshot(name, tmpl, StatusPreparing, "", "", fingerprint, "", "", "", "")
	} else {
		active := detail
		detail = appSnapshot(name, tmpl, StatusPreparing,
			active.Image, active.Fingerprint, fingerprint,
			active.PreparedAt, active.LastReconcileAt, active.LastReconcileStatus, active.LastError)
	}
	detail.UpdatedAt = ts
	return upsertAppTx(ctx, tx, detail)
}

// RecordReconcileSuccess records that an app built and serves an active
// version: status=ready, the new image/fingerprint/prepared_at,
// last_reconcile_status=success AND last_reconcile_at=now (the last meaningful
// reconcile), cleared last_error, and the full configuration snapshot. Because
// the successful generation IS the desired generation, both the active
// Fingerprint and DesiredFingerprint are set to the successful final
// fingerprint, so the next discovery/desired-change starts from a converged
// desired value. On conflict (existing row) the upsert replaces the whole
// snapshot, so a success on a previously-discovered row records its own outcome
// and timestamp.
func (st *State) RecordReconcileSuccess(
	name,
	image,
	fingerprint string,
	preparedAt time.Time,
	fn app.App,
) {
	ctx := context.Background()
	ts := st.nowString()
	prepared := preparedAt.UTC().Format(time.RFC3339)
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail := appSnapshot(name, fn.Template, StatusReady, image, fingerprint, fingerprint, prepared, ts, ReconcileSuccess, "")
		detail.UpdatedAt = ts
		return upsertAppTx(ctx, tx, detail)
	})
	if err != nil {
		st.log.Warn("State: record success failed", "app", name, "error", err)
		return
	}
	st.notifyStatus(name, StatusReady)
}

// RecordPreparing marks the desired configuration as in progress while retaining
// the last active generation and the previous reconcile outcome. It is written
// when a live desired-generation change is detected, before any current-generation
// work runs, so the public status reflects that the app is preparing a new
// generation. The active image/fingerprint/prepared_at are only replaced by a
// successful full reconcile, so a later failure never hides a healthy version.
//
// It is the standalone entry point for callers that do not already hold the
// fingerprint; the fingerprint is computed BEFORE the write transaction opens so
// the transaction holds no filesystem I/O. Callers that already computed it (the
// reconciler's rebuild path) use RecordPreparingWithFingerprint.
func (st *State) RecordPreparing(name string, fn app.App) {
	st.RecordPreparingWithFingerprint(name, fn, st.fingerprint(fn))
}

// RecordPreparingWithFingerprint records a live desired-generation change using a
// fingerprint supplied by the caller, so a caller that already computed it (the
// reconciler's rebuild path) never re-reads the app's source and the write
// transaction holds no external I/O. Like RecordDiscoveredWithFingerprint it is a
// desired-generation write: it refreshes the configuration snapshot and the
// desired fingerprint and sets status=preparing while preserving the last usable
// active generation and the previous last-reconcile outcome.
func (st *State) RecordPreparingWithFingerprint(name string, fn app.App, fingerprint string) {
	ctx := context.Background()
	ts := st.nowString()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		return recordDesiredTx(ctx, tx, name, fn.Template, fingerprint, ts)
	})
	if err != nil {
		st.log.Warn("State: record preparing failed", "app", name, "error", err)
		return
	}
	// recordDesiredTx writes StatusPreparing through the same desired-generation
	// path as discovery, so the observer is notified with preparing here too.
	st.notifyStatus(name, StatusPreparing)
}

// RecordReconcileBuilding changes only the lifecycle status to building. It is
// called at the actual managed-runtime image build boundary (an app or
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
// pass never calls it: an unchanged app stays ready rather than flashing
// reconciling.
func (st *State) RecordReconciling(name string) {
	st.recordStatus(name, StatusReconciling)
}

func (st *State) recordStatus(name, status string) {
	ctx := context.Background()
	ts := st.nowString()
	wrote := false
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at FROM apps WHERE name = ?`, name))
		if err != nil || !found {
			return err
		}
		detail.Status, detail.UpdatedAt = status, ts
		payload, err := marshalApp(detail)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE apps SET data = jsonb(?), updated_at = ? WHERE name = ?`, payload, ts, name); err != nil {
			return err
		}
		wrote = true
		return nil
	})
	if err != nil {
		st.log.Warn("State: record status failed", "app", name, "status", status, "error", err)
		return
	}
	if wrote {
		st.notifyStatus(name, status)
	}
}

// RecordReconcileFailure records that a reconcile build failed.
//
// Key state model: the image/fingerprint/prepared_at of the PREVIOUS active
// version are deliberately left intact so the last good build still serves, and
// the desired fingerprint is preserved too (the failed generation is still the
// desired one, so a later pass can retry it). Only
// last_reconcile_at/last_reconcile_status (failed) and last_error/updated_at
// change. The rest of the persisted snapshot is preserved by a read-modify-write
// inside the transaction. Status is degraded when a prior usable active
// generation exists — the app still serves but the desired generation did
// not converge — and unavailable otherwise; a failure never claims ready, so the
// failed outcome is always visible to an operator.
func (st *State) RecordReconcileFailure(name string, err2 error) {
	ctx := context.Background()
	ts := st.nowString()
	status := ""
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = ?`, name))
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
		// A failed attempt never displaces the last healthy generation: it keeps
		// serving as degraded when a prior usable generation exists, and the
		// app is unavailable when there is none. The failure outcome is never
		// hidden behind a ready status.
		if detail.hasUsableGeneration() {
			detail.Status = StatusDegraded
		} else {
			detail.Status = StatusUnavailable
		}
		status = detail.Status
		detail.UpdatedAt = ts
		payload, err := marshalApp(detail)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`UPDATE apps SET data = jsonb(?), updated_at = ? WHERE name = ?`,
			payload, ts, name)
		return err
	})
	if err != nil {
		st.log.Warn("State: record failure failed", "app", name, "error", err)
		return
	}
	if status != "" {
		st.notifyStatus(name, status)
	}
}

// RecordInvalidDesired records that an app's desired definition is present
// on disk but cannot be loaded or validated (an invalid path, a missing
// template.yaml, or an invalid/unreadable template). It is the state view of an
// INVALID desired state, distinct from RecordReconcileFailure, which records a
// valid desired generation that failed to prepare:
//
//   - The last usable ACTIVE generation (image/fingerprint/prepared_at) is
//     preserved untouched, so a previously-serving version is not erased and a
//     still-serving image stays in the startup image keep-set / retire guards.
//     Execution itself is never driven from state: the caller keeps the invalid
//     app out of the runtime registry.
//   - The DESIRED fingerprint is CLEARED (and, when the prior row carries a
//     template-derived configuration snapshot — handlers/schedules/services/env/
//     secrets/resources — it is cleared too): the invalid definition cannot be
//     parsed, so retaining the old desired digest or configuration would falsely
//     present stale template-derived fields as if they described the current
//     (invalid) config.
//   - last_reconcile_status=failed, last_reconcile_at=now, and last_error=err
//     are persisted. Status is degraded when a usable active generation exists
//     (the old version still serves) and unavailable otherwise (nothing was ever
//     prepared).
//
// It is an UPSERT: an invalid app never seen before (no prior row) is
// inserted, so a fresh invalid desired state is visible to operators instead of
// being silently absent. The status observer is notified with the resulting
// status, keeping the one-hot app_status gauge consistent.
func (st *State) RecordInvalidDesired(name string, err2 error) {
	ctx := context.Background()
	ts := st.nowString()
	status := ""
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = ?`, name))
		if err != nil {
			return err
		}
		if !found {
			// A present invalid entry with no prior row: insert a truthful row
			// with no active generation and no configuration snapshot.
			detail = Detail{Row: Row{Name: name}}
		}
		// An invalid desired definition has no trustworthy desired digest or
		// template-derived configuration: clear them so stale fields are never
		// presented as the current config. The active generation is preserved.
		detail.Runtime = ""
		detail.DesiredFingerprint = ""
		detail.Handlers = nil
		detail.Schedules = nil
		detail.Services = nil
		detail.Env = nil
		detail.Secrets = nil
		detail.Resources = nil
		detail.LastReconcileAt = ts
		detail.LastReconcileStatus = ReconcileFailed
		detail.LastError = err2.Error()
		// Degraded when a usable active generation exists (it still serves),
		// unavailable otherwise; an invalid desired state never reports ready.
		if detail.hasUsableGeneration() {
			detail.Status = StatusDegraded
		} else {
			detail.Status = StatusUnavailable
		}
		status = detail.Status
		detail.UpdatedAt = ts
		payload, err := marshalApp(detail)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO apps (name, data, updated_at) VALUES (?, jsonb(?), ?)
			 ON CONFLICT(name) DO UPDATE SET
			   data       = excluded.data,
			   updated_at = excluded.updated_at`,
			name, payload, ts)
		return err
	})
	if err != nil {
		st.log.Warn("State: record invalid desired failed", "app", name, "error", err)
		return
	}
	if status != "" {
		st.notifyStatus(name, status)
	}
}

// ActiveImages returns the currently recorded active image reference for every
// app that has one, keyed by app name. It is a narrow read seam for
// the startup image keep-set: the caller can preserve an image for an app
// whose desired definition is invalid (so it never reached the loaded set) from
// the persisted active generation alone. Apps with no recorded image (never
// prepared, or an image-less no-runtime service-only generation) are omitted.
// It is a best-effort read: any error is logged and yields nil, never fatal.
func (st *State) ActiveImages() map[string]string {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx, `SELECT name, `+jsonPayloadExpr+` FROM apps`)
	if err != nil {
		st.log.Warn("State: list active images failed", "error", err)
		return nil
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var (
			name string
			data sql.NullString
		)
		if err := rows.Scan(&name, &data); err != nil {
			st.log.Warn("State: scan active image failed", "error", err)
			return out
		}
		detail, err := unmarshalApp(data.String)
		if err != nil {
			st.log.Warn("State: read app payload failed", "app", name, "error", err)
			continue
		}
		if detail.Image != "" {
			out[name] = detail.Image
		}
	}
	return out
}

// RecordServiceFailure records a service convergence failure without hiding a
// healthy app image. An app with a usable active generation is
// degraded; one without an active generation is unavailable. A no-runtime
// external-image service-only app has no app image but a
// successfully-prepared generation, so it is degraded, not unavailable.
func (st *State) RecordServiceFailure(name string, err2 error) {
	ctx := context.Background()
	ts := st.nowString()
	status := ""
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = ?`, name))
		if err != nil || !found {
			return err
		}
		detail.LastReconcileAt = ts
		detail.LastReconcileStatus = ReconcileFailed
		detail.LastError = err2.Error()
		if detail.hasUsableGeneration() {
			detail.Status = StatusDegraded
		} else {
			detail.Status = StatusUnavailable
		}
		status = detail.Status
		detail.UpdatedAt = ts
		payload, err := marshalApp(detail)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`UPDATE apps SET data = jsonb(?), updated_at = ? WHERE name = ?`,
			payload, ts, name)
		return err
	})
	if err != nil {
		st.log.Warn("State: record service failure failed", "app", name, "error", err)
		return
	}
	if status != "" {
		st.notifyStatus(name, status)
	}
}

// RecordRemoved deletes an app and its per-app stats from the state
// database, so a removed app never leaves a stale stats row behind. On
// success the status observer is notified with an EMPTY status, the signal to
// delete the app's status series (a removed app has no lifecycle).
func (st *State) RecordRemoved(name string) {
	ctx := context.Background()
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		return removeTx(ctx, tx, name)
	})
	if err != nil {
		st.log.Warn("State: record removed failed", "app", name, "error", err)
		return
	}
	st.notifyStatus(name, "")
}

// PruneRemoved removes state rows for every app recorded in the database
// that no longer exists in dir (the authoritative /apps root), including
// its app_stats. The filesystem is the source of truth; this only removes
// rows for apps genuinely absent from disk. Transient stat errors
// (permissions/I/O) are skipped — a flaky read must not drop an app that is
// still on disk, mirroring the reconciler's removal tolerance. A row for a
// Relay-owned staging name (app.IsReservedDir, e.g. git's ".sync-*") is
// stale debris from a buggy discovery and is removed unconditionally, even if a
// transient directory of that name happens to exist; no legitimate app name
// can be reserved. The global single-row stats table is deliberately untouched.
// It is intended to run at
// startup, before the fresh registry's counters are seeded from the persisted
// app_stats (restorePersistedStats), so an app removed while the worker
// was down is pruned before its stale app_stats row could be re-seeded into
// metrics.
func (st *State) PruneRemoved(dir string) {
	ctx := context.Background()

	// Collect every recorded name up front and close the rows before deleting:
	// the delete transaction below acquires its own connection from the pool, and
	// fully consuming the query first keeps the read and write paths independent.
	rows, err := st.db.QueryContext(ctx, `SELECT name FROM apps ORDER BY name`)
	if err != nil {
		st.log.Warn("State: prune removed: list apps failed", "error", err)
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
		// A reserved Relay-owned staging name (git's ".sync-*") is
		// definitionally not an app, so any persisted row for it is stale
		// debris from a buggy discovery and must be pruned even if a (transient)
		// directory of that name happens to exist. No legitimate app name
		// can be reserved (ValidName forbids a leading '.'), so this can never
		// drop a real app.
		if app.IsReservedDir(name) {
			if rerr := st.rebuildTx(ctx, func(tx *sql.Tx) error {
				return removeTx(ctx, tx, name)
			}); rerr != nil {
				st.log.Warn("State: prune reserved row failed", "app", name, "error", rerr)
			} else {
				st.notifyStatus(name, "")
			}
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, name)); os.IsNotExist(err) {
			// Reuse the same single-app removal path as RecordRemoved so the
			// live reconciler and the startup sweep behave identically.
			if rerr := st.rebuildTx(ctx, func(tx *sql.Tx) error {
				return removeTx(ctx, tx, name)
			}); rerr != nil {
				st.log.Warn("State: prune removed failed", "app", name, "error", rerr)
				continue
			}
			// Notify the observer (empty status) so an app pruned at startup
			// has its status series removed alongside the row.
			st.notifyStatus(name, "")
			st.log.Info("State: app pruned at startup", "app", name)
		}
		// Any other stat error (permissions/I/O) is skipped: only a genuine
		// os.IsNotExist means the app was removed from the filesystem.
	}
}

// removeTx deletes an app and its per-app stats row — the apps
// row itself and the matching app_stats row — inside tx. The nested
// handler/schedule/service configuration lives inside apps.data, so no
// child-table delete is needed. It is shared by the live reconciler removal
// (RecordRemoved) and the startup sweep (PruneRemoved) so both are behaviorally
// identical: a removed app never leaves a stale app_stats row behind.
func removeTx(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM app_stats WHERE app_name = ?`, name); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM apps WHERE name = ?`, name)
	return err
}

// ListApps returns the app summaries sorted by name, decoding the
// relational name/updated_at columns and the apps.data snapshot for each
// row. A nil/empty slice and nil error mean the state database is simply empty.
// A row whose stored snapshot is corrupt is logged (naming the app and the
// decode error) and skipped, so one bad row cannot hide the others.
func (st *State) ListApps() []Row {
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx,
		`SELECT name, `+jsonPayloadExpr+`, updated_at
		 FROM apps ORDER BY name`)
	if err != nil {
		st.log.Warn("State: list apps failed", "error", err)
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
		detail, err := unmarshalApp(data.String)
		if err != nil {
			st.log.Warn("State: read app payload failed", "app", name, "error", err)
			continue
		}
		detail.Name = name
		detail.UpdatedAt = updatedAt
		detail.HandlerCount = len(detail.Handlers)
		out = append(out, detail.Row)
	}
	return out
}

// GetApp returns the full detail for name, or (zero, false) if unknown. A
// corrupt persisted snapshot fails clearly: the decode error is logged and the
// row reads as unknown rather than yielding a partial record.
func (st *State) GetApp(name string) (Detail, bool) {
	ctx := context.Background()
	detail, found, err := scanApp(name, st.db.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+`, updated_at
		 FROM apps WHERE name = ?`, name))
	if err != nil {
		st.log.Warn("State: get failed", "app", name, "error", err)
		return Detail{}, false
	}
	if !found {
		return Detail{}, false
	}
	return detail, true
}

// scanApp decodes one apps row selected as (json(data), updated_at)
// into a Detail. name is relational metadata supplied by the caller (it is not
// part of the payload). found is false for an absent row; a stored payload whose
// JSON is invalid is returned as an error so the caller can log a clear decode
// failure.
func scanApp(name string, row *sql.Row) (Detail, bool, error) {
	var data sql.NullString
	var updatedAt sql.NullString
	if err := row.Scan(&data, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return Detail{}, false, nil
		}
		return Detail{}, false, err
	}
	detail, err := unmarshalApp(data.String)
	if err != nil {
		return Detail{}, false, err
	}
	detail.Name = name
	detail.UpdatedAt = updatedAt.String
	detail.HandlerCount = len(detail.Handlers)
	return detail, true, nil
}

// upsertAppTx writes the whole Detail snapshot as one JSONB value in
// apps.data, upserting on the name key. The snapshot replaces the previous
// one atomically, so a template change never leaves a mixture of old and new
// handler/schedule/service configuration.
func upsertAppTx(ctx context.Context, tx *sql.Tx, detail Detail) error {
	payload, err := marshalApp(detail)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO apps (name, data, updated_at) VALUES (?, jsonb(?), ?)
		 ON CONFLICT(name) DO UPDATE SET
		   data       = excluded.data,
		   updated_at = excluded.updated_at`,
		detail.Name, payload, detail.UpdatedAt)
	return err
}

// appSnapshot builds the persisted snapshot for an app template: the
// lifecycle fields passed in PLUS the whole configuration (env/secret
// references, handlers, schedules, services). Name is relational
// metadata and UpdatedAt is stamped by the caller; HandlerCount is derived on
// read. Secret entries hold only the reference name — never a resolved value —
// and env entries hold only the env-var name plus a redaction marker, never the
// literal value.
func appSnapshot(
	name string,
	tmpl *app.Template,
	status,
	image,
	fingerprint,
	desiredFingerprint,
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
		Image:              image,
		Fingerprint:        fingerprint,
		DesiredFingerprint: desiredFingerprint,
		LastReconcileAt:    reconcileAt,
		LastError:          lastError,
		Handlers:           snapshotHandlers(tmpl),
		Schedules:          snapshotSchedules(tmpl),
		Services:           snapshotServices(tmpl),
		Env:                env,
		Secrets:            secrets,
		Resources:          snapshotResources(tmpl),
	}
}

// snapshotResources renders the template's EFFECTIVE per-container resource
// limits as the persisted view: memory in bytes, cpus as a core count, and the
// PID limit. It always returns a value (the defaults apply when the template
// omits `resources`), because the effective limits are what the runtime applies.
// The values are configuration, never secrets.
func snapshotResources(tmpl *app.Template) *Resources {
	limits := tmpl.ResourceLimits()
	return &Resources{
		MemoryBytes: limits.MemoryBytes,
		CPUs:        limits.CPUs(),
		Pids:        limits.PidsLimit,
	}
}

// RedactedEnvValue is the fixed marker stored as every env value in the
// persisted snapshot. The state database is a local file an operator can read,
// so it records env-var NAMES only (structural keys); the literal template
// value is never written. Keeping the map shape (name -> marker), rather than
// a bare key list, preserves the internal JSON shape and lets the CLI render
// the configured names without any code-path branching.
const RedactedEnvValue = "[redacted]"

// snapshotConfig copies a template's env and secret MAPPINGS for the persisted
// snapshot. Only the env/secret MAPPINGS are stored (env-var name → secret
// reference, and env-var name → the fixed RedactedEnvValue marker) — never a
// literal env value and never a resolved secret value. Empty maps stay nil so
// the payload omits them and a read-back yields nil (the CLI's "section
// absent" convention).
func snapshotConfig(tmpl *app.Template) (env, secrets map[string]string) {
	if len(tmpl.Env) > 0 {
		env = make(map[string]string, len(tmpl.Env))
		for name := range tmpl.Env {
			env[name] = RedactedEnvValue
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
func snapshotHandlers(tmpl *app.Template) []Handler {
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

// snapshotSchedules renders the template's cron schedules with their stable
// name, verbatim expression, effective location name, resolved timeout, and
// retry count. It is ordered by name (the schedule identity), mirroring the
// identity the cron registration and occurrence dedup use.
func snapshotSchedules(tmpl *app.Template) []Schedule {
	if len(tmpl.Schedules) == 0 {
		return nil
	}
	out := make([]Schedule, 0, len(tmpl.Schedules))
	for _, s := range tmpl.Schedules {
		// time.Location.String() is nil-safe (a nil location reports "UTC"),
		// matching the persisted timezone semantics of the former table.
		out = append(out, Schedule{
			Name:     s.Name,
			Handler:  s.Handler,
			Cron:     s.Cron,
			Timezone: s.Location.String(),
			Timeout:  s.Timeout,
			Retries:  s.Retries,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// snapshotServices renders the template's persistent services with their stable
// name, source (exactly one of entrypoint/image), routing host/path, effective
// port, and desired replicas. It is ordered by name (the service identity).
func snapshotServices(tmpl *app.Template) []Service {
	if len(tmpl.Services) == 0 {
		return nil
	}
	out := make([]Service, 0, len(tmpl.Services))
	for _, s := range tmpl.Services {
		out = append(out, Service{
			Name:       s.Name,
			Entrypoint: s.Entrypoint,
			Image:      s.Image,
			Host:       s.Host,
			Path:       s.Path,
			Port:       s.Port,
			Replicas:   s.Replicas,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
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

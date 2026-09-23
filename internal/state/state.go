package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"relay/internal/function"
)

// DBPath is the fixed internal location of the local state database. It is an
// application convention, not env-configurable: the compose file volume-mounts
// /var/lib/relay so the file survives container restarts, and Open MkdirAll's
// the parent so host-side runs (and tests) work without it existing first.
const DBPath = "/var/lib/relay/db.sqlite3"

// status values for the functions.status column.
const (
	StatusReady   = "ready"
	StatusPending = "pending"
)

// last reconcile status values for functions.last_reconcile_status.
const (
	ReconcileSuccess = "success"
	ReconcileFailed  = "failed"
)

// Row is the per-function summary returned by ListFunctions.
type Row struct {
	Name                string
	Runtime             string
	Status              string
	HandlerCount        int
	LastReconcileStatus string
	PreparedAt          string
	UpdatedAt           string
}

// Detail is the full per-function record returned by GetFunction, including the
// handler list (name + timeout).
type Detail struct {
	Row
	Image           string
	Fingerprint     string
	LastReconcileAt string
	LastError       string
	Handlers        []Handler
	Schedules       []Schedule
	// Services lists the function's persistent services. It is configuration
	// metadata (like the schedules): each entry holds the effective source
	// (entrypoint file, Dockerfile path, or image reference), internal TCP
	// port, and desired replica count. It is nil when the template defines
	// none.
	Services []Service
	// Env and Secrets are the function's env/secret MAPPINGS from its template:
	// env-var name → literal value, and env-var name → secret reference. They
	// are configuration metadata (like the handler timeouts), never secret
	// VALUES — a secret's value is never stored in SQLite. Both are nil when the
	// template defines none.
	Env     map[string]string
	Secrets map[string]string
}

// Handler is one rule's handler and its resolved timeout.
type Handler struct {
	Name    string
	Timeout time.Duration
}

// Schedule is one cron schedule's handler, its verbatim 5-field cron
// expression, the effective timezone name (e.g. "UTC", "Europe/Rome"), and its
// resolved per-invocation timeout.
type Schedule struct {
	Handler  string
	Cron     string
	Timezone string
	Timeout  time.Duration
}

// Service is one persistent service's effective configuration as persisted
// from the template: its source (exactly one of the entrypoint file, the
// Dockerfile path, or the external image reference), the optional routing path
// prefix, its internal TCP port, and the desired replica count. Its identity is
// the configured source descriptor (SourceRef), i.e. whichever of
// Entrypoint/Build/Image is set; it is derived, never stored as a separate
// field.
type Service struct {
	Entrypoint string
	Build      string
	Image      string
	Path       string
	Port       int
	Replicas   int
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
	c := &State{db: db, log: fallbackLogger}

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
	if err := c.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return c, nil
}

// SetLogger redirects log output; used by tests and CLI wiring. A nil l (or a
// nil receiver) is a no-op, leaving the current logger in place.
func (c *State) SetLogger(l *slog.Logger) {
	if c != nil && l != nil {
		c.log = l
	}
}

// Close releases the underlying connection pool. It is non-fatal on error.
func (c *State) Close() error { return c.db.Close() }

// initSchema creates the current tables if they do not already exist, so a
// fresh database is initialized with the current schema. Plain SQL: these
// tables are internal local state and are (re)created on first open.
func (c *State) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS functions (
			name TEXT PRIMARY KEY,
			runtime TEXT,
			status TEXT,
			image TEXT,
			fingerprint TEXT,
			prepared_at TEXT,
			last_reconcile_at TEXT,
			last_reconcile_status TEXT,
			last_error TEXT,
			updated_at TEXT,
			env TEXT,
			secrets TEXT
		)`,
		// handlers is keyed by function_name but carries no foreign key: cleanup
		// is explicit (removeTx), not relational. A relational ON DELETE CASCADE
		// was considered but rejected: it would require PRAGMA foreign_keys=ON on
		// every pooled connection (modernc applies DSN pragmas per connection),
		// and — decisively — the data model lets
		// function_stats rows exist for a name without a functions row
		// (RecordFunctionStats is a standalone upsert used by the CLI/tests and by
		// callers that snapshot per-function counters directly), which FK
		// enforcement would reject. Keep the explicit deletes: they are the tested
		// contract.
		`CREATE TABLE IF NOT EXISTS handlers (
			function_name TEXT,
			handler TEXT,
			timeout TEXT,
			PRIMARY KEY (function_name, handler)
		)`,
		// schedules is keyed by function_name but carries no foreign key, like
		// handlers: cleanup is explicit (removeTx), not relational, for the same
		// reasoning documented above (function stats rows can exist without a
		// functions row, and no PRAGMA foreign_keys is forced).
		`CREATE TABLE IF NOT EXISTS schedules (
			function_name TEXT,
			handler TEXT,
			cron TEXT,
			timezone TEXT,
			timeout TEXT,
			PRIMARY KEY (function_name, handler, cron, timezone)
		)`,
		// services is keyed by function_name but carries no foreign key, like
		// handlers and schedules: cleanup is explicit (removeTx), not relational,
		// for the same reasoning documented above (function stats rows can exist
		// without a functions row, and no PRAGMA foreign_keys is forced). The
		// primary key is the configured source — exactly one of
		// entrypoint/build/image is non-empty (the others are the empty string,
		// never NULL, so the composite stays unique) — which is the same stable
		// identity used on containers and routing (see function.Service.SourceRef).
		`CREATE TABLE IF NOT EXISTS services (
			function_name TEXT,
			entrypoint TEXT,
			build TEXT,
			image TEXT,
			path TEXT,
			port INTEGER,
			replicas INTEGER,
			PRIMARY KEY (function_name, entrypoint, build, image)
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
			data TEXT,
			updated_at TEXT
		)`,
		// function_stats holds the per-function operational snapshot, keyed by
		// function name. It mirrors the global stats table but attributes each
		// payload to a single function (see FunctionStats for the semantics).
		// The key function_name and the write timestamp updated_at stay
		// relational; the payload — including the cumulative warm-container pool
		// counters (warm_acquires_total, cold_starts_total, discarded_total) and
		// the four last_*_at execution-history timestamps — lives in the JSON
		// data column (see stats_json.go). Backlog metrics
		// (pending_entries/oldest_pending_age) stay global-only in stats: they
		// describe the stream backlog, not any one function. The live pool
		// gauges are deliberately never persisted.
		`CREATE TABLE IF NOT EXISTS function_stats (
			function_name TEXT PRIMARY KEY,
			data TEXT,
			updated_at TEXT
		)`,
	}
	for _, s := range stmts {
		if _, err := c.db.ExecContext(ctx, s); err != nil {
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
func (c *State) nowString() string {
	if c.nowFn != nil {
		return c.nowFn().UTC().Format(time.RFC3339)
	}
	return now()
}

// rebuildTx runs fn inside a transaction, which the rebuild path uses so a
// partial scan never leaves a half-populated state database.
func (c *State) rebuildTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := c.db.BeginTx(ctx, nil)
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
func (c *State) empty(ctx context.Context) (bool, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM functions`).Scan(&n)
	return n == 0, err
}

// RebuildFromFS populates an empty state database by scanning dir with the real
// function loader and per-function fingerprinting. Newly loaded functions are
// recorded as status=pending (loaded, not yet built/verified). It is a no-op
// when the state database already has rows — /functions is the source of truth,
// but only for (re)seeding a fresh database.
func (c *State) RebuildFromFS(dir string) error {
	ctx := context.Background()
	populated, err := c.empty(ctx)
	if err != nil {
		return fmt.Errorf("check state empty: %w", err)
	}
	if !populated {
		return nil
	}

	l := function.NewLoader(dir, c.log)
	fns, err := l.Load()
	if err != nil {
		return fmt.Errorf("load functions for state: %w", err)
	}

	// Compute every fingerprint BEFORE opening the write transaction: the
	// transaction must hold no external I/O (filesystem reads) while it is open,
	// so the tx body only writes. Fingerprint errors are logged and fall back to
	// fp="" exactly as before.
	type fpFn struct {
		fn function.Function
		fp string
	}
	prepared := make([]fpFn, 0, len(fns))
	for _, fn := range fns {
		fp, ferr := function.Fingerprint(fn.Dir)
		if ferr != nil {
			c.log.Warn("State: fingerprint failed", "function", fn.Name, "error", ferr)
			fp = ""
		}
		prepared = append(prepared, fpFn{fn: fn, fp: fp})
	}

	return c.rebuildTx(ctx, func(tx *sql.Tx) error {
		for _, p := range prepared {
			ts := c.nowString()
			env, secrets := serializeMappings(p.fn.Template)
			if err := insertStmt(tx)(
				p.fn.Name, p.fn.Template.Runtime, StatusPending, "", p.fp,
				"", "", "", "", ts, env, secrets,
			); err != nil {
				return err
			}
			if err := replaceHandlers(tx, p.fn.Name, p.fn.Template); err != nil {
				return err
			}
			if err := replaceSchedules(tx, p.fn.Name, p.fn.Template); err != nil {
				return err
			}
			if err := replaceServices(tx, p.fn.Name, p.fn.Template); err != nil {
				return err
			}
		}
		return nil
	})
}

// RecordDiscovered records a function discovered from /functions on a fresh
// state database (or when no row exists). It sets runtime/status=pending, the
// fingerprint, and the handlers, clearing any stale prior state. It is an
// upsert keyed by name.
func (c *State) RecordDiscovered(fn function.Function) {
	ctx := context.Background()
	ts := c.nowString()
	// Compute the fingerprint BEFORE the write transaction: the tx must hold no
	// external I/O (filesystem reads), so the closure only writes. Fingerprint
	// errors are logged and fall back to fp="" exactly as before.
	fp, ferr := function.Fingerprint(fn.Dir)
	if ferr != nil {
		c.log.Warn("State: fingerprint failed", "function", fn.Name, "error", ferr)
		fp = ""
	}
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		env, secrets := serializeMappings(fn.Template)
		if err := insertStmt(tx)(fn.Name, fn.Template.Runtime, StatusPending, "", fp, "", "", "", "", ts, env, secrets); err != nil {
			return err
		}
		if err := replaceHandlers(tx, fn.Name, fn.Template); err != nil {
			return err
		}
		if err := replaceSchedules(tx, fn.Name, fn.Template); err != nil {
			return err
		}
		return replaceServices(tx, fn.Name, fn.Template)
	})
	if err != nil {
		c.log.Warn("State: record discovered failed", "function", fn.Name, "error", err)
	}
}

// RecordReconcileSuccess records that a function built and serves an active
// version: status=ready, the new image/fingerprint/prepared_at,
// last_reconcile_status=success AND last_reconcile_at=now (the last meaningful
// reconcile), cleared last_error, and the handlers replaced. On conflict
// (existing row) the upsert persists these outcome columns too, so a success on
// a previously-discovered row records its own outcome and timestamp.
func (c *State) RecordReconcileSuccess(name, image, fingerprint string, preparedAt time.Time, fn function.Function) {
	ctx := context.Background()
	ts := c.nowString()
	prepared := preparedAt.UTC().Format(time.RFC3339)
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		env, secrets := serializeMappings(fn.Template)
		if err := insertStmt(tx)(
			name, fn.Template.Runtime, StatusReady, image, fingerprint,
			prepared, ts, ReconcileSuccess, "", ts, env, secrets,
		); err != nil {
			return err
		}
		if err := replaceHandlers(tx, name, fn.Template); err != nil {
			return err
		}
		if err := replaceSchedules(tx, name, fn.Template); err != nil {
			return err
		}
		return replaceServices(tx, name, fn.Template)
	})
	if err != nil {
		c.log.Warn("State: record success failed", "function", name, "error", err)
	}
}

// RecordReconcileFailure records that a reconcile build failed.
//
// Key state model: the image/fingerprint/prepared_at of the PREVIOUS active
// version are deliberately left intact so the last good build still serves;
// only last_reconcile_at/last_reconcile_status (failed) and last_error/updated_at
// change. Status stays as-is (ready if it was ready). The function is never
// marked unavailable because of a failed rebuild.
func (c *State) RecordReconcileFailure(name string, err2 error) {
	ctx := context.Background()
	ts := c.nowString()
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE functions
			 SET last_reconcile_at = ?, last_reconcile_status = ?, last_error = ?, updated_at = ?
			 WHERE name = ?`,
			ts, ReconcileFailed, err2.Error(), ts, name)
		return err
	})
	if err != nil {
		c.log.Warn("State: record failure failed", "function", name, "error", err)
	}
}

// RecordRemoved deletes a function, its handlers, and its per-function stats
// from the state database, so a removed function never leaves a stale stats row
// behind.
func (c *State) RecordRemoved(name string) {
	ctx := context.Background()
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		return removeTx(ctx, tx, name)
	})
	if err != nil {
		c.log.Warn("State: record removed failed", "function", name, "error", err)
	}
}

// PruneRemoved removes state rows for every function recorded in the database
// that no longer exists in dir (the authoritative /functions root), including
// its handlers and function_stats. The filesystem is the source of truth; this
// only removes rows for functions genuinely absent from disk. Transient stat
// errors (permissions/I/O) are skipped — a flaky read must not drop a function
// that is still on disk, mirroring the reconciler's removal tolerance. The
// global single-row stats table is deliberately untouched. It is intended to
// run at startup, before the fresh registry's counters are seeded from the
// persisted function_stats (restorePersistedStats), so a function removed while
// the worker was down is pruned before its stale function_stats row could be
// re-seeded into metrics.
func (c *State) PruneRemoved(dir string) {
	ctx := context.Background()

	// Collect every recorded name up front and close the rows before deleting:
	// the delete transaction below acquires its own connection from the pool, and
	// fully consuming the query first keeps the read and write paths independent.
	rows, err := c.db.QueryContext(ctx, `SELECT name FROM functions ORDER BY name`)
	if err != nil {
		c.log.Warn("State: prune removed: list functions failed", "error", err)
		return
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			c.log.Warn("State: prune removed: scan name failed", "error", err)
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
			if rerr := c.rebuildTx(ctx, func(tx *sql.Tx) error {
				return removeTx(ctx, tx, name)
			}); rerr != nil {
				c.log.Warn("State: prune removed failed", "function", name, "error", rerr)
				continue
			}
			c.log.Info("State: function pruned at startup", "function", name)
		}
		// Any other stat error (permissions/I/O) is skipped: only a genuine
		// os.IsNotExist means the function was removed from the filesystem.
	}
}

// removeTx deletes a function and all of its state rows — handlers, schedules,
// services, function_stats, and the functions row itself — inside tx. It is
// shared by the live reconciler removal (RecordRemoved) and the startup sweep
// (PruneRemoved) so both are behaviorally identical: a removed function never
// leaves a stale handlers, schedules, services, or function_stats row behind.
func removeTx(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM handlers WHERE function_name = ?`, name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM schedules WHERE function_name = ?`, name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM services WHERE function_name = ?`, name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM function_stats WHERE function_name = ?`, name); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM functions WHERE name = ?`, name)
	return err
}

// ListFunctions returns the function summaries sorted by name. A nil/empty
// slice and nil error mean the state database is simply empty.
func (c *State) ListFunctions() []Row {
	ctx := context.Background()
	rows, err := c.db.QueryContext(ctx,
		`SELECT f.name, f.runtime, f.status, f.prepared_at, f.last_reconcile_status, f.updated_at,
		        COUNT(h.handler)
		 FROM functions f
		 LEFT JOIN handlers h ON h.function_name = f.name
		 GROUP BY f.name
		 ORDER BY f.name`)
	if err != nil {
		c.log.Warn("State: list functions failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(
			&r.Name,
			&r.Runtime,
			&r.Status,
			&r.PreparedAt,
			&r.LastReconcileStatus,
			&r.UpdatedAt,
			&r.HandlerCount,
		); err != nil {
			c.log.Warn("State: scan list failed", "error", err)
			return out
		}
		out = append(out, r)
	}
	return out
}

// GetFunction returns the full detail for name, or (zero, false) if unknown.
func (c *State) GetFunction(name string) (Detail, bool) {
	ctx := context.Background()
	d := Detail{}
	var envJSON, secretsJSON sql.NullString
	err := c.db.QueryRowContext(ctx,
		`SELECT name, runtime, status, image, fingerprint, prepared_at, last_reconcile_at, last_reconcile_status, last_error, updated_at, env, secrets
		 FROM functions WHERE name = ?`, name,
	).Scan(
		&d.Name, &d.Runtime, &d.Status, &d.Image, &d.Fingerprint, &d.PreparedAt,
		&d.LastReconcileAt, &d.LastReconcileStatus, &d.LastError, &d.UpdatedAt,
		&envJSON, &secretsJSON,
	)
	if err == sql.ErrNoRows {
		return Detail{}, false
	}
	if err != nil {
		c.log.Warn("State: get failed", "function", name, "error", err)
		return Detail{}, false
	}
	// Decode the env/secret MAPPINGS (never values). A NULL or unparseable
	// column yields a nil map, which is harmless.
	if envJSON.Valid && envJSON.String != "" {
		_ = json.Unmarshal([]byte(envJSON.String), &d.Env)
	}
	if secretsJSON.Valid && secretsJSON.String != "" {
		_ = json.Unmarshal([]byte(secretsJSON.String), &d.Secrets)
	}

	hrows, err := c.db.QueryContext(ctx,
		`SELECT handler, timeout FROM handlers WHERE function_name = ? ORDER BY handler`, name)
	if err != nil {
		c.log.Warn("State: handlers read failed", "function", name, "error", err)
		return d, true
	}
	defer hrows.Close()
	for hrows.Next() {
		var hn, ht string
		if err := hrows.Scan(&hn, &ht); err != nil {
			c.log.Warn("State: scan handler failed", "function", name, "error", err)
			continue
		}
		dur, derr := time.ParseDuration(ht)
		if derr != nil {
			dur = 0
		}
		d.Handlers = append(d.Handlers, Handler{Name: hn, Timeout: dur})
	}

	srows, err := c.db.QueryContext(ctx,
		`SELECT handler, cron, timezone, timeout FROM schedules WHERE function_name = ? ORDER BY handler, cron`, name)
	if err != nil {
		c.log.Warn("State: schedules read failed", "function", name, "error", err)
		return d, true
	}
	defer srows.Close()
	for srows.Next() {
		var sh, sc, stz, sto string
		if err := srows.Scan(&sh, &sc, &stz, &sto); err != nil {
			c.log.Warn("State: scan schedule failed", "function", name, "error", err)
			continue
		}
		dur, derr := time.ParseDuration(sto)
		if derr != nil {
			dur = 0
		}
		d.Schedules = append(d.Schedules, Schedule{Handler: sh, Cron: sc, Timezone: stz, Timeout: dur})
	}

	srows2, err := c.db.QueryContext(ctx,
		`SELECT entrypoint, build, image, path, port, replicas FROM services WHERE function_name = ? ORDER BY entrypoint, build, image`, name)
	if err != nil {
		c.log.Warn("State: services read failed", "function", name, "error", err)
		return d, true
	}
	defer srows2.Close()
	for srows2.Next() {
		// The three source columns are never NULL: exactly one holds the source
		// descriptor and the others are the empty string (see the schema).
		var se, sb, si string
		var sePath sql.NullString
		var sp, sr int
		if err := srows2.Scan(&se, &sb, &si, &sePath, &sp, &sr); err != nil {
			c.log.Warn("State: scan service failed", "function", name, "error", err)
			continue
		}
		d.Services = append(d.Services, Service{
			Entrypoint: se,
			Build:      sb,
			Image:      si,
			Path:       sePath.String,
			Port:       sp,
			Replicas:   sr,
		})
	}
	return d, true
}

// insertStmt returns a function that INSERTs a function row, upserting
// (replacing) on conflict keyed by name. On conflict only the non-active fields
// are overwritten; a rebuild/discovery never clobbers a ready image until a
// later success records it. The reconcile outcome columns
// (last_reconcile_at/last_reconcile_status/last_error) are also overwritten by
// the record in the VALUES row, so a success on an existing row persists its
// own outcome and timestamp; only the active-version fields
// (image/fingerprint/prepared_at — and status on the failure path) are guarded.
// env and secrets are the serialized env/secret MAPPINGS (JSON objects), never
// secret values.
type insertFn func(
	name, runtime, status, image, fingerprint, prepared string,
	reconcileAt, reconcileStatus, lastError, updated string,
	env, secrets string,
) error

func insertStmt(tx *sql.Tx) insertFn {
	return func(
		name, runtime, status, image, fingerprint, prepared string,
		reconcileAt, reconcileStatus, lastError, updated string,
		env, secrets string,
	) error {
		_, err := tx.Exec(
			`INSERT INTO functions (name, runtime, status, image, fingerprint, prepared_at, last_reconcile_at, last_reconcile_status, last_error, updated_at, env, secrets)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(name) DO UPDATE SET
			   runtime = excluded.runtime,
			   status = excluded.status,
			   image = excluded.image,
			   fingerprint = excluded.fingerprint,
			   prepared_at = excluded.prepared_at,
			   last_reconcile_at = excluded.last_reconcile_at,
			   last_reconcile_status = excluded.last_reconcile_status,
			   last_error = excluded.last_error,
			   updated_at = excluded.updated_at,
			   env = excluded.env,
			   secrets = excluded.secrets`,
			name, runtime, status, image, fingerprint, prepared, reconcileAt, reconcileStatus, lastError, updated, env, secrets)
		return err
	}
}

// replaceHandlers deletes a function's handlers and re-inserts them from the
// template, so the handler list always mirrors the latest parsed template.
func replaceHandlers(tx *sql.Tx, name string, tmpl *function.Template) error {
	if _, err := tx.Exec(`DELETE FROM handlers WHERE function_name = ?`, name); err != nil {
		return err
	}
	for _, rule := range tmpl.Events {
		if _, err := tx.Exec(
			`INSERT OR REPLACE INTO handlers (function_name, handler, timeout) VALUES (?,?,?)`,
			name, rule.Handler, rule.Timeout.String()); err != nil {
			return err
		}
	}
	return nil
}

// replaceSchedules deletes a function's schedules and re-inserts them from the
// template, so the schedule list always mirrors the latest parsed template. The
// timezone is stored as its effective location name (e.g. "UTC",
// "Europe/Rome"); the timeout as its string form.
func replaceSchedules(tx *sql.Tx, name string, tmpl *function.Template) error {
	if _, err := tx.Exec(`DELETE FROM schedules WHERE function_name = ?`, name); err != nil {
		return err
	}
	for _, s := range tmpl.Schedules {
		if _, err := tx.Exec(
			`INSERT OR REPLACE INTO schedules (function_name, handler, cron, timezone, timeout) VALUES (?,?,?,?,?)`,
			name, s.Handler, s.Cron, s.Location.String(), s.Timeout.String()); err != nil {
			return err
		}
	}
	return nil
}

// replaceServices deletes a function's services and re-inserts them from the
// template, so the service list always mirrors the latest parsed template. The
// key is the configured source — exactly one of entrypoint/build/image is
// non-empty (the others are stored as empty strings, never NULL) — which is the
// service's identity. Path is stored as its canonical string (empty = host-only
// routing); port and replicas as their effective integer values (defaults
// included).
func replaceServices(tx *sql.Tx, name string, tmpl *function.Template) error {
	if _, err := tx.Exec(`DELETE FROM services WHERE function_name = ?`, name); err != nil {
		return err
	}
	for _, s := range tmpl.Services {
		if _, err := tx.Exec(
			`INSERT OR REPLACE INTO services (function_name, entrypoint, build, image, path, port, replicas) VALUES (?,?,?,?,?,?,?)`,
			name, s.Entrypoint, s.Build, s.Image, s.Path, s.Port, s.Replicas); err != nil {
			return err
		}
	}
	return nil
}

// serializeMappings renders a template's env and secrets maps as JSON object
// strings for the functions table. Only the MAPPINGS are stored (env-var name →
// literal value, and env-var name → secret reference) — never a secret VALUE.
// JSON is used (rather than a "k=v" logfmt) because env values may legitimately
// contain '=' (e.g. connection strings). An empty map serializes to "null",
// which is stored as NULL.
func serializeMappings(tmpl *function.Template) (env, secrets string) {
	if len(tmpl.Env) > 0 {
		if b, err := json.Marshal(tmpl.Env); err == nil {
			env = string(b)
		}
	}
	if len(tmpl.Secrets) > 0 {
		refs := make(map[string]string, len(tmpl.Secrets))
		for name, ref := range tmpl.Secrets {
			refs[name] = ref.String()
		}
		if b, err := json.Marshal(refs); err == nil {
			secrets = string(b)
		}
	}
	return env, secrets
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
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return t.Format("2006-01-02")
	}
}

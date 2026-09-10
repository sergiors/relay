package state

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	ReconcileSkipped = "skipped"
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
}

// Handler is one rule's handler and its resolved timeout.
type Handler struct {
	Name    string
	Timeout time.Duration
}

// State is a concrete SQLite-backed local state view. It is safe for use from
// the reconciler goroutines; every operation uses a fresh connection context so
// no state is shared.
type State struct {
	db  *sql.DB
	log *log.Logger
}

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
	c := &State{db: db, log: log.Default()}

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

// SetLogger redirects log output; used by tests and CLI wiring.
func (c *State) SetLogger(l *log.Logger) {
	if l != nil {
		c.log = l
	}
}

// Close releases the underlying connection pool. It is non-fatal on error.
func (c *State) Close() error { return c.db.Close() }

// initSchema creates the tables idempotently. Plain SQL, no migration
// framework: these tables are internal local state and are safe to recreate.
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
			updated_at TEXT
		)`,
		// handlers is keyed by function_name but carries no foreign key: cleanup
		// is explicit (removeTx), not relational. A relational ON DELETE CASCADE
		// was considered but rejected: it would require PRAGMA foreign_keys=ON on
		// every pooled connection (modernc applies DSN pragmas per connection) and
		// migrating existing databases, and — decisively — the data model lets
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
		// stats holds the single "current operational snapshot" consumed by
		// Relay itself: monotonically increasing counters persisted across
		// restarts plus gauge snapshots of the pending backlog, never a
		// time-series. All columns are INTEGER except updated_at TEXT.
		`CREATE TABLE IF NOT EXISTS stats (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			events_processed_total INTEGER NOT NULL DEFAULT 0,
			handler_success_total INTEGER NOT NULL DEFAULT 0,
			handler_failure_total INTEGER NOT NULL DEFAULT 0,
			retry_total INTEGER NOT NULL DEFAULT 0,
			dlq_total INTEGER NOT NULL DEFAULT 0,
			pending_entries INTEGER NOT NULL DEFAULT 0,
			oldest_pending_age_seconds INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT
		)`,
		// function_stats holds the per-function operational snapshot, keyed by
		// function name. It mirrors the global stats table but attributes each
		// counter to a single function (see FunctionStats for the semantics).
		// Backlog metrics (pending_entries/oldest_pending_age) stay global-only
		// in stats: they describe the stream backlog, not any one function.
		`CREATE TABLE IF NOT EXISTS function_stats (
			function_name TEXT PRIMARY KEY,
			events_processed_total INTEGER NOT NULL DEFAULT 0,
			handler_success_total INTEGER NOT NULL DEFAULT 0,
			handler_failure_total INTEGER NOT NULL DEFAULT 0,
			retry_total INTEGER NOT NULL DEFAULT 0,
			dlq_total INTEGER NOT NULL DEFAULT 0,
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

// now returns the current UTC time in RFC3339.
func now() string { return time.Now().UTC().Format(time.RFC3339) }

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
			c.log.Printf("state: fingerprint %q: %v", fn.Name, ferr)
			fp = ""
		}
		prepared = append(prepared, fpFn{fn: fn, fp: fp})
	}

	return c.rebuildTx(ctx, func(tx *sql.Tx) error {
		for _, p := range prepared {
			ts := now()
			if err := insertStmt(tx)(p.fn.Name, p.fn.Template.Runtime, StatusPending, "", p.fp, "", "", "", "", ts); err != nil {
				return err
			}
			if err := replaceHandlers(tx, p.fn.Name, p.fn.Template); err != nil {
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
	ts := now()
	// Compute the fingerprint BEFORE the write transaction: the tx must hold no
	// external I/O (filesystem reads), so the closure only writes. Fingerprint
	// errors are logged and fall back to fp="" exactly as before.
	fp, ferr := function.Fingerprint(fn.Dir)
	if ferr != nil {
		c.log.Printf("state: fingerprint %q: %v", fn.Name, ferr)
		fp = ""
	}
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		if err := insertStmt(tx)(fn.Name, fn.Template.Runtime, StatusPending, "", fp, "", "", "", "", ts); err != nil {
			return err
		}
		return replaceHandlers(tx, fn.Name, fn.Template)
	})
	if err != nil {
		c.log.Printf("state: record discovered %q: %v", fn.Name, err)
	}
}

// RecordReconcileSuccess records that a function built and serves an active
// version: status=ready, the new image/fingerprint/prepared_at, last reconcile
// success, cleared last_error, and the handlers replaced.
func (c *State) RecordReconcileSuccess(name, image, fingerprint string, preparedAt time.Time, fn function.Function) {
	ctx := context.Background()
	ts := now()
	prepared := preparedAt.UTC().Format(time.RFC3339)
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		if err := insertStmt(tx)(name, fn.Template.Runtime, StatusReady, image, fingerprint, prepared, ts, ReconcileSuccess, "", ts); err != nil {
			return err
		}
		return replaceHandlers(tx, name, fn.Template)
	})
	if err != nil {
		c.log.Printf("state: record success %q: %v", name, err)
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
	ts := now()
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE functions
			 SET last_reconcile_at = ?, last_reconcile_status = ?, last_error = ?, updated_at = ?
			 WHERE name = ?`,
			ts, ReconcileFailed, err2.Error(), ts, name)
		return err
	})
	if err != nil {
		c.log.Printf("state: record failure %q: %v", name, err)
	}
}

// RecordSkipped records that a reconcile skipped an unchanged function, but
// only if the row already exists (never introduces a phantom row).
func (c *State) RecordSkipped(name string) {
	ctx := context.Background()
	ts := now()
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE functions SET last_reconcile_status = ?, updated_at = ? WHERE name = ?`,
			ReconcileSkipped, ts, name)
		return err
	})
	if err != nil {
		c.log.Printf("state: record skipped %q: %v", name, err)
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
		c.log.Printf("state: record removed %q: %v", name, err)
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
		c.log.Printf("state: prune removed: list functions: %v", err)
		return
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			c.log.Printf("state: prune removed: scan name: %v", err)
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
				c.log.Printf("state: prune removed %q: %v", name, rerr)
				continue
			}
			c.log.Printf("state: function %q removed (pruned at startup)", name)
		}
		// Any other stat error (permissions/I/O) is skipped: only a genuine
		// os.IsNotExist means the function was removed from the filesystem.
	}
}

// removeTx deletes a function and all of its state rows — handlers,
// function_stats, and the functions row itself — inside tx. It is shared by the
// live reconciler removal (RecordRemoved) and the startup sweep (PruneRemoved)
// so both are behaviorally identical: a removed function never leaves a stale
// handlers or function_stats row behind.
func removeTx(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM handlers WHERE function_name = ?`, name); err != nil {
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
		c.log.Printf("state: list functions: %v", err)
		return nil
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.Name, &r.Runtime, &r.Status, &r.PreparedAt, &r.LastReconcileStatus, &r.UpdatedAt, &r.HandlerCount); err != nil {
			c.log.Printf("state: scan list: %v", err)
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
	err := c.db.QueryRowContext(ctx,
		`SELECT name, runtime, status, image, fingerprint, prepared_at, last_reconcile_at, last_reconcile_status, last_error, updated_at
		 FROM functions WHERE name = ?`, name,
	).Scan(&d.Name, &d.Runtime, &d.Status, &d.Image, &d.Fingerprint, &d.PreparedAt, &d.LastReconcileAt, &d.LastReconcileStatus, &d.LastError, &d.UpdatedAt)
	if err == sql.ErrNoRows {
		return Detail{}, false
	}
	if err != nil {
		c.log.Printf("state: get %q: %v", name, err)
		return Detail{}, false
	}

	hrows, err := c.db.QueryContext(ctx,
		`SELECT handler, timeout FROM handlers WHERE function_name = ? ORDER BY handler`, name)
	if err != nil {
		c.log.Printf("state: handlers %q: %v", name, err)
		return d, true
	}
	defer hrows.Close()
	for hrows.Next() {
		var hn, ht string
		if err := hrows.Scan(&hn, &ht); err != nil {
			c.log.Printf("state: scan handler %q: %v", name, err)
			continue
		}
		dur, derr := time.ParseDuration(ht)
		if derr != nil {
			dur = 0
		}
		d.Handlers = append(d.Handlers, Handler{Name: hn, Timeout: dur})
	}
	return d, true
}

// insertStmt returns a function that INSERTs a function row, upserting
// (replacing) on conflict keyed by name. On conflict only the non-active fields
// are overwritten; a rebuild/discovery never clobbers a ready image until a
// later success records it.
type insertFn func(name, runtime, status, image, fingerprint, prepared, reconcileAt, reconcileStatus, lastError, updated string) error

func insertStmt(tx *sql.Tx) insertFn {
	return func(name, runtime, status, image, fingerprint, prepared, reconcileAt, reconcileStatus, lastError, updated string) error {
		_, err := tx.Exec(
			`INSERT INTO functions (name, runtime, status, image, fingerprint, prepared_at, last_reconcile_at, last_reconcile_status, last_error, updated_at)
			 VALUES (?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(name) DO UPDATE SET
			   runtime = excluded.runtime,
			   status = excluded.status,
			   image = excluded.image,
			   fingerprint = excluded.fingerprint,
			   prepared_at = excluded.prepared_at,
			   last_reconcile_status = excluded.last_reconcile_status,
			   last_error = excluded.last_error,
			   updated_at = excluded.updated_at`,
			name, runtime, status, image, fingerprint, prepared, reconcileAt, reconcileStatus, lastError, updated)
		return err
	}
}

// replaceHandlers deletes a function's handlers and re-inserts them from the
// template, so the handler list always mirrors the latest parsed template.
func replaceHandlers(tx *sql.Tx, name string, tmpl *function.Template) error {
	if _, err := tx.Exec(`DELETE FROM handlers WHERE function_name = ?`, name); err != nil {
		return err
	}
	for _, rule := range tmpl.Rules {
		if _, err := tx.Exec(
			`INSERT OR REPLACE INTO handlers (function_name, handler, timeout) VALUES (?,?,?)`,
			name, rule.Handler, rule.Timeout.String()); err != nil {
			return err
		}
	}
	return nil
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

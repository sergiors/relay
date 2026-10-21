package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/app"
)

// schemaLockKey is the PostgreSQL advisory-lock key that serializes concurrent
// schema initialization across processes. Two Relay workers (or a worker and a
// CLI) opening the same fresh database at once would otherwise race the
// CREATE TABLE IF NOT EXISTS statements — a known PostgreSQL race that can
// surface as a duplicate pg_type/relation error. The lock is transaction-scoped
// so it is released the instant initSchema commits.
const schemaLockKey int64 = 0x72656c6179 // "relay"

// appLockNamespace is the high-order key of the per-app advisory locks (see
// lockAppTx). It partitions them from schemaLockKey (which is the single-bigint
// lock 0x72656c6179, i.e. the pair (0, 0x72656c6179)) and from any other
// advisory lock Relay may take in the same database, so an app name can never
// alias the schema-init lock. The pair (appLockNamespace, hash(app name)) is the
// two-int form of pg_advisory_xact_lock.
const appLockNamespace int32 = 1

// appLockKey maps an app name to the low-order half of its advisory-lock key.
// The mapping only needs to be stable within a run; a 32-bit hash collision
// merely over-serializes two unrelated apps and never affects correctness.
func appLockKey(name string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return int32(h.Sum32())
}

// workerLockNamespace tags the per-owner session advisory-lock key so it can
// never alias the schema-init lock (schemaLockKey, whose high byte is 0) or any
// other advisory lock Relay takes. The per-owner locks use the single-bigint
// advisory form, which PostgreSQL keeps in a lock space of its own (distinct
// from the two-int per-app locks, see appLockNamespace), so only the schema lock
// needs the explicit tag.
const workerLockNamespace int64 = 0x52 << 56 // 'R' for Relay owner

// workerLockKey maps a stable worker identity to its session advisory-lock key.
// A 56-bit FNV-1a hash makes an accidental collision between two DIFFERENT
// owners — which would wrongly block the second Open — astronomically unlikely,
// while the namespace tag keeps the key clear of the schema-init lock.
func workerLockKey(owner string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(owner))
	return workerLockNamespace | int64(h.Sum64()&0x00FFFFFFFFFFFFFF)
}

// lockAppTx takes the transaction-scoped per-app advisory lock for name, so
// every read-modify-write of one app's snapshot serializes against the other
// writers of the SAME app — across goroutines, pooled connections, and Relay
// processes. PostgreSQL's MVCC does not serialize two transactions that each
// read the same snapshot and then write different fields: without this lock the
// second writer would replace the first's whole apps.data snapshot, silently
// losing the fields the first had merged in. The lock is keyed on the app NAME,
// not on the row, so it also serializes the app-row-ABSENT case (a SELECT ...
// FOR UPDATE locks nothing when the row does not exist, so it cannot guard a
// create/upsert race). It is released when the enclosing transaction commits or
// rolls back.
func lockAppTx(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1, $2)`, appLockNamespace, appLockKey(name)); err != nil {
		return fmt.Errorf("lock app %q: %w", name, err)
	}
	return nil
}

// Pool defaults used when Options leaves them unset. They mirror
// config.DefaultDatabase* (a leaf package cannot import config, so the values
// are duplicated here as internal defaults).
const (
	defaultMaxOpenConns    = 8
	defaultMaxIdleConns    = 4
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
	defaultConnectTimeout  = 10 * time.Second
)

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

// FencingToken is an opaque, PostgreSQL-authoritative fencing token for one
// app's current desired generation. Zero is never allocated: every positive
// token is drawn from the global apps_fencing_token_seq sequence while the
// per-app advisory lock is held, so it is unique across all Relay processes
// sharing the database. Fencing-token-guarded writes are conditioned on the
// token the caller observed, so a stale completion updates no row instead of
// overwriting a newer desired generation or resurrecting a removed app. It is
// deliberately not part of Detail or the persisted JSONB snapshot: it is
// internal coordination state, never a CLI/JSON field.
type FencingToken int64

// nextFencingTokenTx allocates the next positive fencing token. nextval is
// intentionally non-transactional: a rolled-back write merely skips a value, and
// tokens only need to be unique and increasing, never gapless. Callers invoke it
// while holding lockAppTx for the app whose desired state is advancing.
func nextFencingTokenTx(ctx context.Context, tx *sql.Tx) (FencingToken, error) {
	var fencingToken int64
	if err := tx.QueryRowContext(ctx,
		`SELECT nextval('apps_fencing_token_seq')`).Scan(&fencingToken); err != nil {
		return 0, fmt.Errorf("allocate app fencing token: %w", err)
	}
	return FencingToken(fencingToken), nil
}

// Row is the per-app summary returned by ListApps. Its fields are part
// of the apps.data snapshot except relational Name/UpdatedAt and the derived
// HandlerCount. The app fencing token is separate relational metadata and is
// not part of Row or Detail.
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
// through app_json.go), while name, updated_at, and fencing_token are relational
// columns. The embedded Row fields and every field below — except Name,
// UpdatedAt, and the derived HandlerCount — are part of that snapshot.
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
	// state snapshot (readable by any operator with database access) must never
	// carry them.
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

// State is a concrete PostgreSQL-backed local state view. It is safe for
// concurrent use from the reconciler goroutines and the scheduler: the
// underlying *sql.DB is a bounded connection pool and every operation uses a
// fresh connection context so no state is shared between calls.
type State struct {
	db  *sql.DB
	log *slog.Logger
	// owner is this process's STABLE logical worker identity for the per-worker
	// stats snapshots (the worker passes config.WorkerID). Stats rows are keyed
	// by worker so two workers sharing one database never overwrite each other's
	// counters; CLI reads aggregate across every owner while the worker's
	// startup restore reads only its own row. It is empty for standalone
	// CLI/admin handles, which never own stats rows.
	owner string
	// instance is this process incarnation's ALWAYS-FRESH fencing token
	// (config.InstanceID). It is stored in the stats row and every flush is
	// conditioned on it, so a stale process for the same owner cannot overwrite
	// a newer incarnation's snapshots. It is set only when owner is set (see
	// Open), and empty for CLI/admin handles.
	instance string
	// ownerConn is the dedicated connection that holds this handle's session
	// advisory lock on the worker identity (see acquireWorkerLock). It is nil for
	// an ownerless CLI/admin handle (which never takes the lock). The connection
	// is pinned for the handle's whole lifetime so the lock is never silently
	// returned to the pool mid-run; Close releases it, and PostgreSQL releases it
	// if the process dies first.
	ownerConn *sql.Conn
	// ownerPID is the PostgreSQL backend PID (pg_backend_pid) of ownerConn,
	// captured immediately after the lock is acquired. VerifyOwnerLock compares
	// it so a silently replaced connection (a different backend that happens to
	// lack the lock) is detected as a loss rather than a held lock. Zero for an
	// ownerless handle.
	ownerPID int
	// ownerMu serializes EVERY access to the pinned lock connection (ownerConn):
	// VerifyOwnerLock reads and queries it, and Close closes and clears it.
	// database/sql serializes individual driver calls on a *sql.Conn but NOT a
	// whole check, so two concurrent checks would interleave their calls and pgx
	// would fail the overlap with "conn busy" — a spurious ownership loss; the
	// mutex also stops VerifyOwnerLock reading the field while Close nils it. It
	// is deliberately NARROW: it guards ONLY ownerConn, so every other State
	// operation (the pool, reconcile/stats/outbox writes, CLI reads) stays fully
	// concurrent.
	ownerMu sync.Mutex
	// nowFn is an injectable clock used to stamp updated_at/reconcile
	// timestamps and the schedule_pending retention deadline. Production leaves
	// it nil and falls back to time.Now, preserving behavior exactly; tests
	// inject a fake to advance time deterministically without sleeping. Tests
	// must set it before any writes, so every stamped row uses the fake clock.
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

// Options configures Open. DSN is the PostgreSQL connection string (the
// DATABASE_URL value); Owner is the stable logical worker identity used to scope
// stats rows and Instance is this process incarnation's fencing token. Owner and
// Instance are a PAIR: both set for a worker handle (which owns stats rows) or
// both empty for a CLI/admin handle (which never does); setting exactly one is a
// configuration error. The pool fields are optional and fall back to the package
// defaults when zero.
type Options struct {
	DSN   string
	Owner string
	// Instance is the always-fresh per-process incarnation token paired with
	// Owner. It fences stale incarnations of the same owner out of the stats
	// row (see RecordStatsSnapshot).
	Instance string
	// Logger, when non-nil, is the logger Open installs. Otherwise the package
	// fallback logger is used until SetLogger overrides it.
	Logger *slog.Logger
	// Pool tuning; zero values take the package defaults.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	ConnectTimeout  time.Duration
}

// fallbackLogger is the package-level default logger used when a State is
// opened without SetLogger (e.g. the CLI admin commands and the worker's
// early-open path before wiring). It runs at DEBUG so nothing is hidden; the
// worker replaces it with the process's leveled logger via SetLogger.
var fallbackLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

// Open connects to the PostgreSQL state database, applies bounded pool settings,
// verifies connectivity with a bounded ping, and initializes the schema
// idempotently. Unlike the former file-backed store, a failure here is FATAL to
// the caller: PostgreSQL is a hard requirement (the operator state view, the
// per-worker stats snapshots, and the durable schedule outbox), so Open returns a
// clear, actionable error rather than a degraded handle. Schema initialization is
// serialized by a transaction-scoped advisory lock so concurrent openers cannot
// race the DDL.
//
// A handle with an Owner (a worker) additionally takes an EXCLUSIVE session
// advisory lock on that stable worker identity, held for the handle's lifetime
// and released by Close (or by PostgreSQL when the process/connection dies). A
// second concurrent Open presenting the same WORKER_ID — on this host or any
// other sharing the database — FAILS with a clear error naming the identity,
// instead of silently sharing the identity and fighting over one stats row.
// Ownerless CLI/admin handles never take the lock and are never blocked by a
// running worker.
func Open(ctx context.Context, opts Options) (*State, error) {
	if opts.DSN == "" {
		return nil, fmt.Errorf("open state: no DATABASE_URL configured")
	}
	// Owner and Instance are a pair: a handle either owns stats rows (both set)
	// or does not (both empty). A half-configured identity would make stats
	// ownership ambiguous, so it is rejected before touching the database.
	if (opts.Owner == "") != (opts.Instance == "") {
		return nil, fmt.Errorf("open state: Owner and Instance must be set together")
	}
	db, err := sql.Open("pgx", opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	st := &State{db: db, log: fallbackLogger, owner: opts.Owner, instance: opts.Instance}
	if opts.Logger != nil {
		st.log = opts.Logger
	}

	// A small bounded pool: Relay's state workload is short and low-frequency,
	// and several replicas plus CLI commands may share one database. The values
	// are conservative per-process caps, never a host-wide quota.
	db.SetMaxOpenConns(orDefault(opts.MaxOpenConns, defaultMaxOpenConns))
	db.SetMaxIdleConns(orDefault(opts.MaxIdleConns, defaultMaxIdleConns))
	db.SetConnMaxLifetime(orDuration(opts.ConnMaxLifetime, defaultConnMaxLifetime))
	db.SetConnMaxIdleTime(orDuration(opts.ConnMaxIdleTime, defaultConnMaxIdleTime))

	// Bounded connectivity check: a dead/unreachable database must surface as an
	// actionable startup error, not a hang.
	pingCtx, cancel := context.WithTimeout(ctx, orDuration(opts.ConnectTimeout, defaultConnectTimeout))
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open state: connect to PostgreSQL failed: %w", err)
	}

	if err := st.initSchema(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Take the per-owner EXCLUSIVE session lock (held for the handle's lifetime)
	// BEFORE claiming the stats row: a second concurrent process presenting the
	// same WORKER_ID — on this host or any other sharing the database — fails
	// Open here, without re-stamping the live owner's stats row and thus fencing
	// it. The lock is released by Close, or by PostgreSQL when the pinned
	// connection dies.
	ownerConn, err := st.acquireWorkerLock(pingCtx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	st.ownerConn = ownerConn
	// Record the pinned backend's PID so VerifyOwnerLock can tell the lock
	// connection apart from any replacement: a silently dropped and re-dialed
	// connection would land on a backend that does not hold the lock, and the
	// PID mismatch makes that a detected loss.
	if ownerConn != nil {
		pid, pidErr := backendPID(pingCtx, ownerConn)
		if pidErr != nil {
			_ = ownerConn.Close()
			_ = db.Close()
			return nil, fmt.Errorf("open state: %w", pidErr)
		}
		st.ownerPID = pid
	}
	// Claim ownership of this worker's stats row AFTER the schema exists and the
	// worker lock is held, so a fresh owner gets a row and a restart re-stamps its
	// (possibly next) incarnation without disturbing the prior totals (see
	// claimStatsOwnership).
	if err := st.claimStatsOwnership(pingCtx, ownerConn); err != nil {
		if ownerConn != nil {
			_ = ownerConn.Close()
		}
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func orDefault(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func orDuration(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
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

// Close releases the worker lock (if this handle holds one) and the underlying
// connection pool. Closing the pinned lock connection releases its session
// advisory lock, so a restart on the same WORKER_ID can reopen immediately;
// PostgreSQL also releases the lock if the process dies without a clean Close.
// It is non-fatal on error and safe on a nil receiver.
//
// Close serializes with VerifyOwnerLock through ownerMu: the pinned lock
// connection is never closed or cleared while a verification is querying it. A
// verification queued behind Close observes the cleared connection and fails
// closed. Only the pinned connection is guarded; the pool Close stays outside
// the mutex so unrelated database work is not serialized.
func (st *State) Close() error {
	if st == nil {
		return nil
	}
	st.ownerMu.Lock()
	if st.ownerConn != nil {
		_ = st.ownerConn.Close()
		st.ownerConn = nil
	}
	st.ownerMu.Unlock()
	return st.db.Close()
}

// acquireWorkerLock takes the EXCLUSIVE session advisory lock for this handle's
// worker identity on a dedicated connection, pinned for the State handle's
// lifetime (see Close). One pool connection is therefore reserved for the whole
// run (the pool defaults leave ample headroom); the alternative — taking the
// session lock on a pooled connection — would return the lock to the pool with
// the connection and leak it. It returns (nil, nil) for an ownerless CLI/admin
// handle, which must never be blocked by a running worker. A second process
// presenting the same owner — on this host or any other sharing the database —
// gets a clear error naming the identity, so two instances can never silently
// share a WORKER_ID. The lock is non-blocking (pg_try_advisory_lock): a conflict
// fails fast rather than hanging startup, and PostgreSQL releases a session
// advisory lock automatically when its connection closes, so a crashed worker
// never leaves a stale lock behind.
func (st *State) acquireWorkerLock(ctx context.Context) (*sql.Conn, error) {
	if st.owner == "" {
		return nil, nil
	}
	conn, err := st.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire worker lock: %w", err)
	}
	var locked bool
	if err := conn.QueryRowContext(ctx,
		`SELECT pg_try_advisory_lock($1)`, workerLockKey(st.owner)).Scan(&locked); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire worker lock: %w", err)
	}
	if !locked {
		// Release the borrowed connection before failing: the lock was not taken,
		// so nothing would hold it, but the pool slot must not leak.
		_ = conn.Close()
		return nil, fmt.Errorf(
			"open state: worker identity %q is already in use by another running instance; each concurrent worker must set a distinct WORKER_ID",
			st.owner)
	}
	return conn, nil
}

// initSchema creates the current tables if they do not already exist, so a
// fresh database is initialized with the current schema. It runs inside a
// transaction that first takes a transaction-scoped advisory lock, so
// concurrent openers (two workers starting together, or a CLI racing the worker)
// serialize their DDL instead of racing CREATE TABLE IF NOT EXISTS — a known
// PostgreSQL race. The data columns are native JSONB: a typed Go struct is
// marshalled to JSON text and stored with the jsonb type, and read back as JSON
// text.
//
// apps stores a whole per-app snapshot as one JSONB object in data, with only
// the stable name key, the write timestamp, and the internal fencing token
// (fencing_token) kept as columns. There are deliberately no
// per-handler/per-schedule/per-service child tables: the nested
// configuration is part of the snapshot, so a template change replaces one row
// atomically. stats is the per-worker global counter snapshot (keyed by worker
// identity so shared-database workers never overwrite each other, with an
// instance fencing column), app_stats the per-app counterpart (keyed by app name
// + worker), stats_meta the single-row global reset generation, and
// schedule_pending the durable schedule-publication outbox.
func (st *State) initSchema(ctx context.Context) error {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("init schema: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaLockKey); err != nil {
		return fmt.Errorf("init schema: acquire lock: %w", err)
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS apps (
			name TEXT PRIMARY KEY,
			data JSONB NOT NULL,
			updated_at TEXT NOT NULL,
			fencing_token BIGINT NOT NULL DEFAULT 0
		)`,
		// fencing_token is the internal per-app fencing token (see FencingToken).
		// It is a column, never part of apps.data. The global sequence is CACHE 1
		// so concurrent allocators never hand out the same value; gaps from
		// rolled-back transactions are harmless.
		`CREATE SEQUENCE IF NOT EXISTS apps_fencing_token_seq AS BIGINT CACHE 1 START WITH 1`,
		// stats holds each worker's "current operational snapshot". The row key
		// is the OWNER worker identity, so N workers sharing this database keep N
		// independent counter sets instead of last-writer-wins overwriting one
		// another. instance is the current owning PROCESS incarnation (a fencing
		// value): only the process whose instance matches the stored one may
		// update the row, so a stale incarnation cannot overwrite a newer one.
		// Only stable relational metadata is a column — the worker key, the
		// incarnation, and the write timestamp updated_at — while the evolving
		// counter/gauge payload is the JSONB object in data (see Stats and
		// stats_json.go). JSONB, not a column per field: the payload grows as
		// instrumentation is added without changing the schema, and absent fields
		// decode to zero.
		`CREATE TABLE IF NOT EXISTS stats (
			worker TEXT PRIMARY KEY,
			instance TEXT NOT NULL DEFAULT '',
			data JSONB NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		// stats_meta is the single-row GLOBAL stats epoch (id is pinned to
		// 'global'). generation is bumped by every `stats reset`; a flush reads
		// it under a shared lock and rebases when it observes a bump, so a
		// delayed pre-reset snapshot from ANY worker is fenced out — not only the
		// socket-owning worker's. It carries no statistics; it is coordination
		// state.
		`CREATE TABLE IF NOT EXISTS stats_meta (
			id TEXT PRIMARY KEY CHECK (id = 'global'),
			generation BIGINT NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL
		)`,
		`INSERT INTO stats_meta (id, generation, updated_at) VALUES ('global', 0, '')
		 ON CONFLICT (id) DO NOTHING`,
		// app_stats holds the per-app operational snapshot, keyed by
		// (app_name, worker). It mirrors the global stats table but attributes
		// each payload to a single app AND a single owning worker, so two workers
		// reporting the same app do not clobber each other. The key columns and
		// the write timestamp updated_at stay relational; the payload —
		// including the cumulative warm-container pool counters
		// (warm_acquires_total, cold_starts_total, discarded_total) and the four
		// last_*_at execution-history timestamps — lives in the JSONB data
		// column (see stats_json.go). Backlog metrics
		// (pending_entries/oldest_pending_age) stay global-only per worker: they
		// describe the stream backlog, not any one app. The live pool gauges are
		// deliberately never persisted.
		`CREATE TABLE IF NOT EXISTS app_stats (
			app_name TEXT NOT NULL,
			worker TEXT NOT NULL,
			data JSONB NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (app_name, worker)
		)`,
		// schedule_pending is the durable schedule-publication retry outbox: one
		// row per logical occurrence, written BEFORE its Redis publish
		// (intent-first durability) and deleted once publication resolves. The row
		// key is the derived occurrence ID and the COMPLETE immutable occurrence
		// intent (app/schedule/handler/scheduled_at) is the JSONB object in data,
		// so the schema stays stable while the intent grows. It is NOT a history
		// table and NOT an execution source: /apps stays authoritative, and the
		// row exists only until a later publication resolves (published or a
		// clean duplicate) or its retention expires and it is removed. The
		// scheduling fields (attempts, next_attempt_ms, lease_until_ms) are
		// integer Unix milliseconds used to coordinate retry observations: a row
		// is claimable only while next_attempt_ms and lease_until_ms are both in
		// the past, and a claim sets lease_until_ms atomically so concurrent
		// retriers cannot both claim it. claim_token is a fencing generation: a
		// claim increments it and returns the new value, and a claimant's
		// reschedule/delete is conditioned on the token it observed, so a
		// superseded claimant whose lease already expired can never clobber a
		// newer claimant's lease or delete its row. expires_at_ms is the durable
		// retention deadline (integer Unix milliseconds), stamped at first insert
		// as now + 7 days and never modified by reschedule; a record with
		// expires_at_ms <= now is expired, is excluded from claims, and is deleted
		// by the bounded cleanup instead of retried. The occurrence intent
		// (id/data/expires_at_ms) is never updated after insert.
		`CREATE TABLE IF NOT EXISTS schedule_pending (
			id TEXT PRIMARY KEY,
			data JSONB NOT NULL,
			attempts BIGINT NOT NULL DEFAULT 0,
			next_attempt_ms BIGINT NOT NULL DEFAULT 0,
			lease_until_ms BIGINT NOT NULL DEFAULT 0,
			expires_at_ms BIGINT NOT NULL DEFAULT 0,
			claim_token BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS schedule_pending_due
			ON schedule_pending (next_attempt_ms, lease_until_ms)`,
	}

	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("init schema: commit: %w", err)
	}
	return nil
}

// execQuerier is the minimal execution surface claimStatsOwnership needs. It is
// satisfied by both *sql.DB (ownerless/direct callers) and the pinned *sql.Conn
// the worker lock holds, so the claim runs on the worker-lock connection when one
// is held (see Open) without needing a second pool slot.
type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// claimStatsOwnership records this handle's process incarnation as the current
// owner of its stats row. It is a no-op for a handle with no owner (CLI/admin).
//
// The claim is an unconditional INSERT ... ON CONFLICT DO UPDATE of ONLY the
// instance column. A fresh owner gets a zero row whose updated_at is ” — the
// oldest possible value — so it can never win the freshest-owner backlog gauge
// before its first real flush. An EXISTING row keeps its data and updated_at
// untouched, so a restart's startup restore (OwnerStats) still reads the prior
// cumulative totals before the first flush writes the resumed values back.
//
// Taking over unconditionally is what lets a newer incarnation fence out a stale
// one: once this claim re-stamps the instance, the stale process's next flush
// compares the stored instance and stops writing (see RecordStatsSnapshot). Two
// workers deliberately configured with the SAME WorkerID can no longer coexist:
// Open's per-owner exclusive lock refuses the second one before this claim runs,
// so the takeover only happens on a restart after the prior handle closed (see
// acquireWorkerLock). The instance fence remains as defense in depth for the
// case where a handle's lock connection was lost while it kept running.
func (st *State) claimStatsOwnership(ctx context.Context, q execQuerier) error {
	if st.owner == "" {
		return nil
	}
	payload, err := marshalStats(Stats{})
	if err != nil {
		return fmt.Errorf("claim stats ownership: %w", err)
	}
	// Run on the pinned worker-lock connection when one is held (see Open): the
	// claim must be ordered AFTER the lock is taken, and reusing that connection
	// needs no second pool slot.
	if _, err := q.ExecContext(ctx,
		`INSERT INTO stats (worker, instance, data, updated_at) VALUES ($1, $2, $3, '')
		 ON CONFLICT(worker) DO UPDATE SET instance = excluded.instance`,
		st.owner, st.instance, payload); err != nil {
		return fmt.Errorf("claim stats ownership: %w", err)
	}
	return nil
}

// Owner returns this handle's stable stats-owner identity (the worker
// config.WorkerID in production, empty for CLI/admin handles). It is exposed so
// the worker can log the identity its persisted stats are scoped to.
func (st *State) Owner() string { return st.owner }

// nowString returns the current time as an RFC3339 UTC string, using the
// injectable clock when set and the package default otherwise. It is the single
// timestamp source for every State write, so an injected clock (tests) governs
// every updated_at/last_reconcile_at consistently.
func (st *State) nowString() string {
	return st.nowTime().UTC().Format(time.RFC3339)
}

// nowTime returns the current instant as a time.Time from the same injectable
// clock nowString uses, defaulting to time.Now when no clock is installed. It is
// the time-valued counterpart of nowString, for writes whose persisted form is
// integer Unix milliseconds rather than an RFC3339 string (the schedule_pending
// retention deadline).
func (st *State) nowTime() time.Time {
	if st.nowFn != nil {
		return st.nowFn()
	}
	return time.Now()
}

// rebuildTx runs fn inside a transaction, which the rebuild path uses so a
// partial scan never leaves a half-populated state database. database/sql
// exposes no context-aware Commit, so the ctx is checked immediately before
// committing: a lifecycle canceled after the last statement rolls back instead
// of persisting a write that raced shutdown.
func (st *State) rebuildTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		// database/sql has no context-aware Commit. If the lifecycle was
		// canceled while the commit was in flight, surface the cancellation so
		// callers can classify the write as shutdown rather than a state failure.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(err, ctxErr)
		}
		return err
	}
	return nil
}

// appWriteTx runs fn inside a short transaction that first takes the per-app
// advisory lock for name (see lockAppTx), so every write to one app's snapshot
// serializes with every other write to the SAME app. It is the transaction
// helper for every apps-table writer: the read-modify-write paths (status
// transitions, desired-generation refreshes, failure/invalid records) each read
// the current snapshot, change a subset of fields, and write the whole payload
// back, so two concurrent writers without this lock would lose each other's
// fields; the full-snapshot writers (a successful reconcile) take the same lock
// so they cannot interleave with a read-modify-write and be clobbered by it. It
// holds no external I/O — fingerprints are computed by the caller before the
// transaction opens.
func (st *State) appWriteTx(ctx context.Context, name string, fn func(tx *sql.Tx) error) error {
	return st.rebuildTx(ctx, func(tx *sql.Tx) error {
		if err := lockAppTx(ctx, tx, name); err != nil {
			return err
		}
		return fn(tx)
	})
}

// logAppWriteFailure reports a non-fatal apps-table write failure. A write
// interrupted by lifecycle cancellation is normal shutdown, not a state
// failure, so it is logged at Debug; every other error is a genuine (but
// non-fatal) state-view failure and is logged at Warn. The fixed message is the
// same in both cases, so a real failure stays recognisable.
func (st *State) logAppWriteFailure(msg string, err error, args ...any) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		st.log.Debug(msg, append(args, "error", err)...)
		return
	}
	st.log.Warn(msg, append(args, "error", err)...)
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
//
// ctx bounds the empty check and every write in the seed transaction, so a
// lifecycle cancellation cannot leave a slow startup seed running.
func (st *State) RebuildFromFS(ctx context.Context, dir string) error {
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
//
// ctx bounds the empty check and the seed transaction.
func (st *State) RebuildFromApps(ctx context.Context, discovered []DiscoveredApp) error {
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
//
// Each app's row is written under its per-app advisory lock (see lockAppTx), so
// a seed never interleaves with a concurrent per-app write. The loader order is
// stable, so the locks are always acquired in the same order.
func (st *State) rebuildDiscoveredTx(ctx context.Context, discovered []DiscoveredApp) error {
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		for _, d := range discovered {
			if err := lockAppTx(ctx, tx, d.App.Name); err != nil {
				return err
			}
			fencingToken, err := nextFencingTokenTx(ctx, tx)
			if err != nil {
				return err
			}
			detail := appSnapshot(d.App.Name, d.App.Template, StatusPreparing, "", "", d.Fingerprint, "", "", "", "")
			detail.UpdatedAt = st.nowString()
			if err := upsertAppTx(ctx, tx, detail, fencingToken); err != nil {
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
//
// It returns the new fencing token to carry through this generation's
// build/service callbacks, or zero with an error when the write failed (in
// which case those callbacks must not persist anything). ctx bounds the write
// transaction, including its advisory-lock wait, so a cancelled lifecycle
// cannot block a startup or reload path.
func (st *State) RecordDiscovered(ctx context.Context, fn app.App) (FencingToken, error) {
	return st.RecordDiscoveredWithFingerprint(ctx, fn, st.fingerprint(fn))
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
// computed inside the closure. The returned fencing token is the exact token the
// write persisted; callers must not read it back later. A write interrupted by
// ctx cancellation returns zero and an error wrapping the context error.
func (st *State) RecordDiscoveredWithFingerprint(
	ctx context.Context,
	fn app.App,
	fingerprint string,
) (FencingToken, error) {
	ts := st.nowString()
	var fencingToken FencingToken
	err := st.appWriteTx(ctx, fn.Name, func(tx *sql.Tx) error {
		var err error
		fencingToken, err = recordDesiredTx(ctx, tx, fn.Name, fn.Template, fingerprint, ts)
		return err
	})
	if err != nil {
		err = fmt.Errorf("record discovered: %w", err)
		st.logAppWriteFailure("State: record discovered failed", err, "app", fn.Name)
		return 0, err
	}
	st.notifyStatus(fn.Name, StatusPreparing)
	return fencingToken, nil
}

// recordDesiredTx is the shared body of the desired-generation writes (startup
// discovery and live desired-change detection): it allocates a fresh fencing
// token, refreshes the whole configuration snapshot and the desired fingerprint,
// and sets status=preparing while preserving the last usable active generation
// (image/fingerprint/prepared_at) and the previous last-reconcile outcome. Only
// a successful full reconcile replaces the active generation. An absent row is
// created with no active generation and the supplied desired fingerprint.
//
// Advancing the fencing token is what invalidates older completions across
// workers: a callback carrying a previous token updates no row.
func recordDesiredTx(
	ctx context.Context,
	tx *sql.Tx,
	name string,
	tmpl *app.Template,
	fingerprint,
	ts string,
) (FencingToken, error) {
	fencingToken, err := nextFencingTokenTx(ctx, tx)
	if err != nil {
		return 0, err
	}
	detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+`, updated_at FROM apps WHERE name = $1`, name))
	if err != nil {
		return 0, err
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
	if err := upsertAppTx(ctx, tx, detail, fencingToken); err != nil {
		return 0, err
	}
	return fencingToken, nil
}

// RecordReconcileSuccess records that an app built and serves an active
// version: status=ready, the new image/fingerprint/prepared_at,
// last_reconcile_status=success AND last_reconcile_at=now (the last meaningful
// reconcile), cleared last_error, and the full configuration snapshot. Because
// the successful generation IS the desired generation, both the active
// Fingerprint and DesiredFingerprint are set to the successful final
// fingerprint, so the next discovery/desired-change starts from a converged
// desired value.
//
// It is a fencing-token-guarded UPDATE, never an upsert: a completion carrying a
// stale token (another worker advanced the app, or the app was removed) affects
// no row and is not published to the status observer — it reports (false, nil).
// A success can only ever describe the generation that already owns the row. It
// returns whether the write landed and an error only when the write itself
// failed. ctx bounds the transaction, including its advisory-lock wait.
func (st *State) RecordReconcileSuccess(
	ctx context.Context,
	name,
	image,
	fingerprint string,
	preparedAt time.Time,
	fn app.App,
	fencingToken FencingToken,
) (bool, error) {
	if fencingToken <= 0 {
		return false, nil
	}
	ts := st.nowString()
	prepared := preparedAt.UTC().Format(time.RFC3339)
	wrote := false
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		detail := appSnapshot(name, fn.Template, StatusReady, image, fingerprint, fingerprint, prepared, ts, ReconcileSuccess, "")
		detail.UpdatedAt = ts
		var err error
		wrote, err = updateAppIfCurrentTx(ctx, tx, detail, fencingToken)
		return err
	})
	if err != nil {
		err = fmt.Errorf("record reconcile success: %w", err)
		st.logAppWriteFailure("State: record success failed", err, "app", name)
		return false, err
	}
	if wrote {
		st.notifyStatus(name, StatusReady)
	}
	return wrote, nil
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
func (st *State) RecordPreparing(ctx context.Context, name string, fn app.App) (FencingToken, error) {
	return st.RecordPreparingWithFingerprint(ctx, name, fn, st.fingerprint(fn))
}

// RecordPreparingWithFingerprint records a live desired-generation change using a
// fingerprint supplied by the caller, so a caller that already computed it (the
// reconciler's rebuild path) never re-reads the app's source and the write
// transaction holds no external I/O. Like RecordDiscoveredWithFingerprint it is a
// desired-generation write: it refreshes the configuration snapshot and the
// desired fingerprint and sets status=preparing while preserving the last usable
// active generation and the previous last-reconcile outcome.
//
// It returns the new fencing token to carry through this generation's
// callbacks, or zero with an error when the write failed (fail closed: those
// callbacks must not persist anything). ctx bounds the transaction, including
// its advisory-lock wait.
func (st *State) RecordPreparingWithFingerprint(
	ctx context.Context,
	name string,
	fn app.App,
	fingerprint string,
) (FencingToken, error) {
	ts := st.nowString()
	var fencingToken FencingToken
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		var err error
		fencingToken, err = recordDesiredTx(ctx, tx, name, fn.Template, fingerprint, ts)
		return err
	})
	if err != nil {
		err = fmt.Errorf("record preparing: %w", err)
		st.logAppWriteFailure("State: record preparing failed", err, "app", name)
		return 0, err
	}
	// recordDesiredTx writes StatusPreparing through the same desired-generation
	// path as discovery, so the observer is notified with preparing here too.
	st.notifyStatus(name, StatusPreparing)
	return fencingToken, nil
}

// RecordReconcileBuilding changes only the lifecycle status to building. It is
// called at the actual managed-runtime image build boundary (an app or
// dependency image build), not when a build is merely queued. Persistent
// services have no separate image build, so the service path never publishes
// building. It is fencing-token-guarded: a stale build callback affects no row
// and reports (false, nil). ctx bounds the transaction, including its
// advisory-lock wait, so a cancelled lifecycle returns promptly with a context
// error.
func (st *State) RecordReconcileBuilding(
	ctx context.Context,
	name string,
	fencingToken FencingToken,
) (bool, error) {
	return st.recordStatus(ctx, name, StatusBuilding, fencingToken)
}

// RecordReconciling changes only the lifecycle status to reconciling. It is
// called at the actual convergence seam of a pass that has corrective container
// work to perform (entrypoint/image services), so the status never claims
// convergence work that has not started. A fully-converged no-op verification
// pass never calls it: an unchanged app stays ready rather than flashing
// reconciling. It is fencing-token-guarded: a stale convergence callback affects
// no row and reports (false, nil). ctx bounds the transaction, including its
// advisory-lock wait.
func (st *State) RecordReconciling(
	ctx context.Context,
	name string,
	fencingToken FencingToken,
) (bool, error) {
	return st.recordStatus(ctx, name, StatusReconciling, fencingToken)
}

func (st *State) recordStatus(
	ctx context.Context,
	name,
	status string,
	fencingToken FencingToken,
) (bool, error) {
	if fencingToken <= 0 {
		return false, nil
	}
	ts := st.nowString()
	wrote := false
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = $1 AND fencing_token = $2`, name, int64(fencingToken)))
		if err != nil || !found {
			return err
		}
		detail.Status, detail.UpdatedAt = status, ts
		wrote, err = updateAppIfCurrentTx(ctx, tx, detail, fencingToken)
		return err
	})
	if err != nil {
		err = fmt.Errorf("record status %q: %w", status, err)
		st.logAppWriteFailure("State: record status failed", err, "app", name, "status", status)
		return false, err
	}
	if wrote {
		st.notifyStatus(name, status)
	}
	return wrote, nil
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
// failed outcome is always visible to an operator. It is fencing-token-guarded:
// a stale failure affects no row and is not published (false, nil). It returns
// whether the write landed and an error only when the write itself failed. ctx
// bounds the transaction, including its advisory-lock wait.
func (st *State) RecordReconcileFailure(
	ctx context.Context,
	name string,
	err2 error,
	fencingToken FencingToken,
) (bool, error) {
	if fencingToken <= 0 {
		return false, nil
	}
	ts := st.nowString()
	status := ""
	wrote := false
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = $1 AND fencing_token = $2`, name, int64(fencingToken)))
		if err != nil {
			return err
		}
		if !found {
			// A stale fencing token no longer owns the row; do not resurrect it.
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
		wrote, err = updateAppIfCurrentTx(ctx, tx, detail, fencingToken)
		return err
	})
	if err != nil {
		err = fmt.Errorf("record reconcile failure: %w", err)
		st.logAppWriteFailure("State: record failure failed", err, "app", name)
		return false, err
	}
	if wrote {
		st.notifyStatus(name, status)
	}
	return wrote, nil
}

// RecordInvalidDesired records that an app's desired definition is present
// on disk but cannot be loaded or validated (an invalid path or an
// invalid/unreadable template). It is the state view of an
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
// being silently absent. It advances the fencing token, invalidating older
// completions across workers, and returns that token (zero with an error on a
// failed write). The status observer is notified with the resulting status,
// keeping the one-hot app_status gauge consistent. ctx bounds the transaction,
// including its advisory-lock wait.
func (st *State) RecordInvalidDesired(
	ctx context.Context,
	name string,
	err2 error,
) (FencingToken, error) {
	ts := st.nowString()
	var fencingToken FencingToken
	status := ""
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		nextFencingToken, err := nextFencingTokenTx(ctx, tx)
		if err != nil {
			return err
		}
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = $1`, name))
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
		if err := upsertAppTx(ctx, tx, detail, nextFencingToken); err != nil {
			return err
		}
		fencingToken = nextFencingToken
		return nil
	})
	if err != nil {
		err = fmt.Errorf("record invalid desired: %w", err)
		st.logAppWriteFailure("State: record invalid desired failed", err, "app", name)
		return 0, err
	}
	if status != "" {
		st.notifyStatus(name, status)
	}
	return fencingToken, nil
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
// successfully-prepared generation, so it is degraded, not unavailable. It is
// fencing-token-guarded: a stale callback reports (false, nil) and writes
// nothing. It returns whether the write landed and an error only when the write
// itself failed. ctx bounds the transaction, including its advisory-lock wait.
func (st *State) RecordServiceFailure(
	ctx context.Context,
	name string,
	err2 error,
	fencingToken FencingToken,
) (bool, error) {
	if fencingToken <= 0 {
		return false, nil
	}
	ts := st.nowString()
	status := ""
	wrote := false
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		detail, found, err := scanApp(name, tx.QueryRowContext(ctx,
			`SELECT `+jsonPayloadExpr+`, updated_at
			 FROM apps WHERE name = $1 AND fencing_token = $2`, name, int64(fencingToken)))
		if err != nil {
			return err
		}
		if !found {
			return nil
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
		wrote, err = updateAppIfCurrentTx(ctx, tx, detail, fencingToken)
		return err
	})
	if err != nil {
		err = fmt.Errorf("record service failure: %w", err)
		st.logAppWriteFailure("State: record service failure failed", err, "app", name)
		return false, err
	}
	if wrote {
		st.notifyStatus(name, status)
	}
	return wrote, nil
}

// RecordRemoved deletes an app and its per-app stats from the state
// database, so a removed app never leaves a stale stats row behind. The delete
// is fencing-token-guarded: it removes the row only while the caller's token
// still owns it, so a removal observed by an older worker cannot delete a newer
// desired generation. app_stats is deleted only when the matching apps row was
// actually removed; both happen in one transaction. On a landed delete the status
// observer is notified with an EMPTY status, the signal to delete the app's
// status series (a removed app has no lifecycle). It returns whether the row was
// deleted. ctx bounds the transaction, including its advisory-lock wait, so a
// cancelled lifecycle never blocks a removal.
func (st *State) RecordRemoved(
	ctx context.Context,
	name string,
	fencingToken FencingToken,
) (bool, error) {
	if fencingToken <= 0 {
		return false, nil
	}
	deleted := false
	err := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx,
			`DELETE FROM apps WHERE name = $1 AND fencing_token = $2`,
			name, int64(fencingToken))
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			// A newer fencing token (or a re-added app) owns the row; leave it.
			return nil
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM app_stats WHERE app_name = $1`, name); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if err != nil {
		err = fmt.Errorf("record removed: %w", err)
		st.logAppWriteFailure("State: record removed failed", err, "app", name)
		return false, err
	}
	if deleted {
		st.notifyStatus(name, "")
	}
	return deleted, nil
}

// PruneRemoved removes state rows for every app recorded in the database
// that is no longer an app candidate in dir (the authoritative /apps root),
// including its app_stats. A direct child is an app candidate only when it
// contains template.yaml, so a row is removed when its directory is gone OR the
// directory exists but has no template.yaml. The filesystem is the source of
// truth; this only removes rows for names that are no longer app candidates.
// Transient stat errors (permissions/I/O) are skipped — a flaky read must not
// drop an app whose template may still be present, mirroring the reconciler's
// removal tolerance. A row for a Relay-owned staging name (app.IsReservedDir,
// e.g. git's ".sync-*") is stale debris from a buggy discovery and is removed
// unconditionally, even if a transient directory of that name happens to exist;
// no legitimate app name can be reserved. The global single-row stats table is
// deliberately untouched. It is intended to run at startup, before the fresh
// registry's counters are seeded from the persisted
// app_stats (restorePersistedStats), so an app removed while the worker
// was down is pruned before its stale app_stats row could be re-seeded into
// metrics.
//
// ctx bounds the listing query and every delete transaction. A cancelled ctx
// stops the sweep promptly and returns an error wrapping the context error;
// other per-row failures are logged and the first is returned after the
// remaining rows have been attempted, so one bad row cannot hide the rest.
func (st *State) PruneRemoved(ctx context.Context, dir string) error {
	// Collect every recorded name up front and close the rows before deleting:
	// the delete transaction below acquires its own connection from the pool, and
	// fully consuming the query first keeps the read and write paths independent.
	rows, err := st.db.QueryContext(ctx, `SELECT name FROM apps ORDER BY name`)
	if err != nil {
		err = fmt.Errorf("prune removed: list apps: %w", err)
		st.logAppWriteFailure("State: prune removed: list apps failed", err)
		return err
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			err = fmt.Errorf("prune removed: scan name: %w", err)
			st.logAppWriteFailure("State: prune removed: scan name failed", err)
			return err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		err = fmt.Errorf("prune removed: list apps: %w", err)
		st.logAppWriteFailure("State: prune removed: list apps failed", err)
		return err
	}
	_ = rows.Close()

	var firstErr error
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("prune removed: %w", err)
		}
		// A reserved Relay-owned staging name (git's ".sync-*") is
		// definitionally not an app, so any persisted row for it is stale
		// debris from a buggy discovery and must be pruned even if a (transient)
		// directory of that name happens to exist. No legitimate app name
		// can be reserved (ValidName forbids a leading '.'), so this can never
		// drop a real app.
		reserved := app.IsReservedDir(name)
		if !reserved {
			present, statErr := app.HasTemplate(filepath.Join(dir, name))
			if statErr != nil {
				// Any ambiguous stat error (permissions/I/O, or a non-directory
				// path) is skipped: only a definite "no template.yaml" means the
				// app is no longer a candidate. A flaky read must never drop a
				// row whose definition may still be present.
				continue
			}
			if present {
				continue
			}
		}
		// Startup pruning is filesystem-authoritative and therefore
		// unconditional: no fencing token can own a directory that is
		// genuinely gone at startup. removeTx is the UNCONDITIONAL removal.
		if rerr := st.appWriteTx(ctx, name, func(tx *sql.Tx) error {
			return removeTx(ctx, tx, name)
		}); rerr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fmt.Errorf("prune removed: %w", rerr)
			}
			msg := "State: prune removed failed"
			if reserved {
				msg = "State: prune reserved row failed"
			}
			st.log.Warn(msg, "app", name, "error", rerr)
			if firstErr == nil {
				firstErr = fmt.Errorf("prune removed %q: %w", name, rerr)
			}
			continue
		}
		// Notify the observer (empty status) so an app pruned at startup
		// has its status series removed alongside the row.
		st.notifyStatus(name, "")
		if !reserved {
			st.log.Info("State: app pruned at startup", "app", name)
		}
	}
	return firstErr
}

// removeTx deletes an app and its per-app stats row — the apps
// row itself and the matching app_stats row — inside tx. The nested
// handler/schedule/service configuration lives inside apps.data, so no
// child-table delete is needed. It is the UNCONDITIONAL removal used by the
// startup sweep (PruneRemoved), where the filesystem — not a fencing token — is
// the source of truth. Live removal uses the fencing-token-guarded DELETE in
// RecordRemoved instead.
func removeTx(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM app_stats WHERE app_name = $1`, name); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM apps WHERE name = $1`, name)
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
		 FROM apps WHERE name = $1`, name))
	if err != nil {
		st.log.Warn("State: get failed", "app", name, "error", err)
		return Detail{}, false
	}
	if !found {
		return Detail{}, false
	}
	return detail, true
}

// scanApp decodes one apps row selected as (data::text, updated_at)
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
// apps.data, upserting on the name key and stamping fencing_token. It is reserved
// for desired-state writes (discovery, preparing, invalid), which are the only
// operations allowed to CREATE a row or advance its fencing token. The snapshot
// replaces the previous one atomically, so a template change never leaves a
// mixture of old and new handler/schedule/service configuration.
func upsertAppTx(ctx context.Context, tx *sql.Tx, detail Detail, fencingToken FencingToken) error {
	payload, err := marshalApp(detail)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO apps (name, data, updated_at, fencing_token)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT(name) DO UPDATE SET
		   data          = excluded.data,
		   updated_at    = excluded.updated_at,
		   fencing_token = excluded.fencing_token`,
		detail.Name, payload, detail.UpdatedAt, int64(fencingToken))
	return err
}

// updateAppIfCurrentTx rewrites one app snapshot only while the row still
// carries fencingToken, returning false when the token is stale or the row is
// gone. Every fencing-token-specific outcome (status, success, failure,
// service failure) goes through this guarded UPDATE so a delayed completion
// from another worker — or from before a removal — can never overwrite a newer
// desired generation, create a row, or publish a status.
func updateAppIfCurrentTx(
	ctx context.Context,
	tx *sql.Tx,
	detail Detail,
	fencingToken FencingToken,
) (bool, error) {
	if fencingToken <= 0 {
		return false, nil
	}
	payload, err := marshalApp(detail)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE apps SET data = $1, updated_at = $2
		 WHERE name = $3 AND fencing_token = $4`,
		payload, detail.UpdatedAt, detail.Name, int64(fencingToken))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
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
// persisted snapshot. The state database is shared and readable by any operator
// with access, so it records env-var NAMES only (structural keys); the literal
// template value is never written. Keeping the map shape (name -> marker),
// rather than a bare key list, preserves the internal JSON shape and lets the
// CLI render the configured names without any code-path branching.
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

package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"relay/internal/bytesize"
)

// Config holds the worker's environment-resolved settings, assembled by Load at
// worker startup. The administrative CLI commands do not build a Config; they
// resolve only the values they need through the narrow LoadDatabaseURL loader.
// Every field maps to a single environment variable,
// except the always-generated per-process InstanceID (see below). Application
// code should consume resolved values instead of reading environment variables
// directly.
//
// The required fields name the Redis stream, group, and address the worker
// consumes, the PostgreSQL state DSN (DATABASE_URL), and the worker's stable
// node identity (NODE_ID, surfaced as NodeID); the address values (MetricsAddr,
// GitWebhookAddr) are empty when disabled, so gating on non-empty keeps them
// opt-in.
type Config struct {
	RedisURI    string
	RedisStream string
	RedisGroup  string
	// DatabaseURL is the PostgreSQL DSN (DATABASE_URL) of the shared state
	// database. It is REQUIRED: Relay's operator state view, per-node stats
	// snapshots, and the durable schedule-publication outbox all live in
	// PostgreSQL, so the worker refuses to start without it. The value carries
	// credentials and is never logged. It is consumed by internal/state as the
	// sole connection identity.
	DatabaseURL string
	// NodeID is the worker's REQUIRED, STABLE node identity, resolved verbatim
	// from NODE_ID (trimmed) by ParseNodeID. It is the SAME identity used as
	// the Redis consumer name, the relay.node_id container-ownership label, and
	// the stats node key, so a restart reuses one identity across all three and
	// two nodes sharing a group, database, or daemon never alias. There is no
	// default or hostname fallback: a missing or malformed NODE_ID fails
	// startup. The value is a bounded token (maxNodeIDLen; ASCII letters,
	// digits, '.', '_', '-') because it becomes a stats row key, a Redis
	// consumer name, and a container label.
	NodeID string
	// InstanceID is the ALWAYS-FRESH process identity for this run
	// ("instance-<hex>"). It is regenerated on every Load (never configurable)
	// and stored alongside the owner in the stats row as a fencing value: a
	// stale process incarnation for the same NodeID cannot overwrite a newer
	// one's snapshots. It is internal coordination state, not operator
	// configuration.
	InstanceID string
	// StreamRetention is the source-stream retention window
	// (REDIS_STREAM_RETENTION). Unset defaults to DefaultStreamRetention
	// (24h); an explicitly empty, zero, or negative value disables main-stream
	// retention (0), and a malformed value logs and disables it. The worker
	// trims the configured source stream with XTRIM ... ACKED, so entries are
	// removed only once every consumer group has acknowledged them.
	StreamRetention time.Duration
	// DLQRetention is the durable dead-letter retention window
	// (DLQ_RETENTION). Unset defaults to DefaultDLQRetention (7 days); an
	// explicitly empty, zero, or negative value disables DLQ retention (0), and
	// a malformed value logs and disables it. PostgreSQL is the sole DLQ
	// persistence: the worker deletes DLQ rows older than this window in
	// bounded batches. It is independent of the source-stream retention above.
	DLQRetention time.Duration
	// InvocationRetention is the TERMINAL invocation-state retention window
	// (REDIS_INVOCATION_RETENTION). Unset defaults to
	// DefaultInvocationRetention (48h); an explicitly empty, zero, or negative
	// value disables terminal retention (0), and a malformed value logs and
	// disables it. Unlike StreamRetention/DLQRetention this does not trim a
	// stream: it is the TTL the stream layer applies to a message's
	// invocation-state hash ONLY after the message has left the PEL (a
	// successful XACK on the success/obsolete/DLQ path, or a cleared
	// missing-payload PEL reference). While the message is recoverable the hash
	// stays PERSISTENT regardless of this value. When disabled (0) the terminal
	// marker is still written atomically, so the hash stays persistent and
	// terminal/stale-transition guards are unchanged — only the expiry is
	// omitted.
	InvocationRetention time.Duration
	// StatsRetention is the STALE-OWNER retention window for the per-node
	// statistics rows (STATS_RETENTION). Unset defaults to
	// DefaultStatsRetention (7 days); an explicitly empty, zero, or negative
	// value DISABLES stale-owner compaction (0), and a malformed value logs and
	// disables it. It bounds the rows of DEAD workers: a stats/app_stats row
	// whose owner is not this worker and whose updated_at is older than the
	// window is periodically deleted. It is deliberately independent of the
	// Redis retentions: those trim streams, this compacts operator state. A live
	// worker's own row is never pruned (the owner is excluded), so disabling it
	// only risks unbounded growth from many short-lived random NodeIDs.
	StatsRetention time.Duration
	MetricsAddr    string
	// GitWebhookAddr is the address for the GitHub webhook server
	// (POST /github) that triggers an automatic git sync on matching pushes.
	// Like MetricsAddr it is opt-in via the GIT_WEBHOOK_ADDR environment
	// variable: empty (unset) disables the webhook server entirely, and a
	// non-empty value makes the worker bind it at startup (a bad address is a
	// fatal startup error, matching metrics). Sync still runs through the
	// separate git.Sync entry; the webhook server only schedules a sync through
	// the coalescing trigger in internal/git/webhook, never in the request path.
	GitWebhookAddr string
	// MaxConcurrentInvocations is the MAX_CONCURRENT_INVOCATIONS value (default
	// 8): the total number of app invocations executing concurrently in this
	// single Relay worker. Values below the default fall back in the runner (see
	// runner.SetMaxConcurrentInvocations); it is always positive after Load.
	MaxConcurrentInvocations int
	// MaxConcurrentBuilds is the MAX_CONCURRENT_BUILDS value (default 2): the
	// cap on dependency-image and app-image builds running concurrently in this
	// single Relay worker. It is held only while a build actually runs, so
	// non-build preparation (source selection and snapshot, dependency
	// snapshot, reuse probes) does not consume a slot; a preparation's
	// dependency and app sub-builds remain sequential within its own goroutine.
	// Startup preparation also uses this value as its worker-pool size, so it
	// never starts one goroutine per app.
	// It is INDEPENDENT of MaxConcurrentInvocations and of MAX_BUFFERED_EVENTS,
	// and it is not a host-wide resource quota: each worker enforces its own,
	// so a deployment of N workers can run up to N * MaxConcurrentBuilds
	// builds. It is always positive after Load.
	MaxConcurrentBuilds int
	// MaxWarmContainers is the MAX_WARM_CONTAINERS value (default 8): the hard
	// per-worker bound on warm EXECUTION containers this worker keeps across all
	// apps (idle + busy + in-flight creates), enforced globally rather than per
	// app. It is a count bound on the pooled execution-container population,
	// not an invocation-concurrency limit: when every warm container is busy and
	// none can be evicted, an invocation is left pending (backpressure) rather
	// than starting an extra container. Persistent service containers and
	// stale-version throwaway containers are outside this bound. It is
	// INDEPENDENT of MaxConcurrentInvocations and is startup configuration; each
	// worker enforces its own. It is always positive after Load.
	MaxWarmContainers int
	// MaxBufferedEvents is the MAX_BUFFERED_EVENTS value (default 16): the
	// number of events already read from Redis and still held locally by this
	// worker before they complete/ACK. It bounds the local buffer so the
	// backlog stays in Redis when full. It is always positive after Load.
	MaxBufferedEvents int
	// MaxEventBytes is the MAX_EVENT_BYTES value (default 256KiB, hard max
	// 1MiB): the maximum byte length of a message's raw `event` field. The
	// environment value accepts a human-readable binary size (B/KiB/MiB/GiB)
	// or a bare integer byte count for backwards compatibility (see
	// ParseMaxEventBytes). A message whose raw event value exceeds it is
	// rejected before JSON decode, schedule classification, event matching,
	// and handler execution, and is routed to the DLQ with a bounded
	// diagnostic summary instead of the payload. It bounds the RAW event
	// value only — not the whole Redis entry or its RESP/response overhead,
	// which Redis and go-redis have already materialized before Relay can
	// inspect it. It is always positive after Load: zero is rejected, never
	// read as "unlimited".
	MaxEventBytes int
	// Networks is the NETWORKS value: the ordered, de-duplicated global
	// workload network set. It applies to both execution containers and
	// persistent service containers this worker creates (a routed service
	// additionally joins TraefikNetwork), as one order-independent set. It is
	// empty (nil) when NETWORKS is unset. The names are parsed once at startup
	// (see ParseNetworks) and are startup configuration: changing them requires
	// a worker restart. The networks are infrastructure owned OUTSIDE Relay —
	// Relay verifies they exist at startup and never creates them.
	Networks []string
	// TraefikNetwork is the optional TRAEFIK_NETWORK value: the Docker network
	// Traefik is attached to. It is required only when an app's template
	// service declares a `host` (routed service); Relay never creates the
	// network itself and verifies it exists on every routed reconcile. Empty
	// means routing is not configured (unrouted services are unaffected).
	TraefikNetwork string
	// TraefikEntryPoints is the optional TRAEFIK_ENTRYPOINTS value: one or
	// more comma-separated Traefik entrypoint names (e.g. "websecure" or
	// "web,websecure", passed through verbatim). When set on a routed service
	// Relay generates the Traefik router `entrypoints` label. Empty = the
	// label is omitted (no default).
	TraefikEntryPoints string
	// TraefikCertResolver is the optional TRAEFIK_CERTRESOLVER value (e.g.
	// "letsencrypt"): when set on a routed service Relay generates both the
	// Traefik router `tls=true` and `tls.certresolver` labels. Empty = both
	// labels are omitted (no default; TLS stays off on the router).
	TraefikCertResolver string
	// TraefikPriority is the optional TRAEFIK_PRIORITY value: nil when unset
	// (the router `priority` label is omitted and Traefik's own default
	// priority behavior applies; no value is ever defaulted). When provided
	// it must be a positive integer — an invalid value is a fatal
	// configuration error — so &0 is never produced.
	TraefikPriority *int
	// TraefikHostOverride is the optional TRAEFIK_HOST_OVERRIDE value: the
	// domain suffix substituted for the domain of every routed service's
	// declared host, keeping the host's left-most label (so
	// "issuer.example.com" routes as "issuer.localhost" when set to
	// "localhost"). Empty (unset) means no override: hosts are used verbatim,
	// preserving prior behavior. The value is passed through as-is here and
	// validated by the routing layer (a routing concern); it is hostname-dumb
	// config, like the other TRAEFIK_* fields.
	TraefikHostOverride string
	// WarmContainerIdleTimeout is the WARM_CONTAINER_IDLE_TIMEOUT value: how
	// long a healthy idle warm execution container is kept before the runtime
	// evicts it. Unset/empty defaults to DefaultWarmContainerIdleTimeout (5m);
	// it must be a positive Go duration (e.g. "5m", "90s") — an invalid or
	// non-positive value is a fatal configuration error, matching
	// MAX_CONCURRENT_INVOCATIONS rather than REDIS_STREAM_RETENTION's log-and-disable
	// style (a typo in a container-warmth knob must not silently change runtime
	// behavior). It is always positive after Load.
	WarmContainerIdleTimeout time.Duration
	// HotReload is the optional HOT_RELOAD value (default false): whether a
	// runtime whose dependency layout permits it (Python and Node) builds a
	// source-independent app image and bind-mounts the app's live source tree
	// read-only into execution and entrypoint-service containers, instead of
	// baking the source into the image. It is the public trigger for Relay's
	// internal source-mount mechanism. It is a plain boolean and is parsed with
	// strconv.ParseBool's accepted forms (see ParseBool); an unparseable value
	// is a returned configuration error naming HOT_RELOAD rather than being
	// silently coerced. It is consumed by the runtime layer; this package only
	// resolves and validates it. False (the default) preserves the historical
	// baked-source behavior exactly.
	HotReload bool
}

// Default max-concurrency, max-build-concurrency, and max-buffered-events
// values. The runner and runtime layers keep their own copies of these
// constants (a leaf package cannot import config); this package owns the
// env-facing defaults.
const (
	DefaultMaxConcurrentInvocations = 8
	// DefaultMaxConcurrentBuilds is the MAX_CONCURRENT_BUILDS value and
	// DefaultMaxWarmContainers the MAX_WARM_CONTAINERS value. The runtime
	// package keeps its own mirroring constants (a leaf package cannot import
	// config); this package owns the env-facing defaults.
	DefaultMaxConcurrentBuilds = 2
	DefaultMaxWarmContainers   = 8
	DefaultMaxBufferedEvents   = 16
	// DefaultMaxEventBytes is the byte cap applied to a message's raw `event`
	// value when MAX_EVENT_BYTES is unset/empty. MaxEventBytesLimit is the hard
	// ceiling: a configured or default value above it is rejected, so the cap
	// can never be lifted into an unbounded read/parse.
	DefaultMaxEventBytes = 256 << 10
	MaxEventBytesLimit   = 1 << 20
	// DefaultWarmContainerIdleTimeout is the idle-eviction window applied when
	// WARM_CONTAINER_IDLE_TIMEOUT is unset/empty. The runtime package's own
	// default is only a fallback for direct NewManager callers; the worker
	// always passes config's resolved value.
	DefaultWarmContainerIdleTimeout = 5 * time.Minute
	// DefaultStreamRetention is the source-stream retention window applied when
	// REDIS_STREAM_RETENTION is UNSET. An explicitly empty value still disables
	// retention (see StreamRetention), preserving the historical opt-out. The
	// source trim uses XTRIM ... ACKED, so the default is safe on a shared
	// stream: an entry is removed only once every consumer group has
	// acknowledged it.
	DefaultStreamRetention = 24 * time.Hour
	// DefaultDLQRetention is the durable dead-letter retention window applied
	// when DLQ_RETENTION is UNSET; an explicitly empty value disables it. The
	// DLQ is stored in PostgreSQL, so retention is a bounded age-based delete,
	// never a broker stream trim. Seven days mirrors the schedule outbox's
	// retention. It is INDEPENDENT of the invocation-state terminal TTL below:
	// a DLQ row is durable operator data, a terminal invocation-state hash is
	// per-message Redis state.
	DefaultDLQRetention = 7 * 24 * time.Hour
	// DefaultInvocationRetention is the terminal invocation-state retention
	// window applied when REDIS_INVOCATION_RETENTION is UNSET; an explicitly
	// empty value disables it. It is the TTL applied to a message's
	// invocation-state hash only after the message has left the PEL. The
	// internal/stream package cannot import config, so it keeps a mirroring
	// default (stream.DefaultInvocationRetention), applied when a direct
	// NewConsumer/NewInvocationState caller supplies no explicit window; the
	// worker always passes config's resolved value (see ConsumerConfig).
	// Forty-eight hours is the historical terminal-bookkeeping window after
	// this change (it replaced a hard-coded 7 days).
	DefaultInvocationRetention = 48 * time.Hour
	// DefaultStatsRetention is the stale-owner retention window applied when
	// STATS_RETENTION is UNSET; an explicitly empty value disables compaction.
	// Seven days matches the DLQ and schedule-outbox windows: a dead worker's
	// stats row is retained long enough to cover a restart or a short outage,
	// then compacted so an unbounded stream of short-lived random NodeIDs
	// cannot grow the tables forever. A live worker's row is never pruned.
	DefaultStatsRetention = 7 * 24 * time.Hour
)

// Node identity constants. NODE_ID is REQUIRED and used verbatim (see
// ParseNodeID); the only generated identity is the per-process InstanceID, so
// its form is prefixed to tell it apart from an operator-supplied value, and the
// random suffix is 8 bytes of crypto/rand rendered as hex (collision-free for
// any realistic deployment). maxNodeIDLen bounds NODE_ID so it can never be
// used to smuggle unbounded data into a stats row key, a Redis consumer name, or
// a container label.
const (
	instanceIDPrefix = "instance-"
	maxNodeIDLen     = 128
)

// State-database pool defaults. Relay's state workload is low-frequency and
// short-transactional (reconciler status writes, a 5s stats flush, scheduler
// outbox writes, CLI reads), so a small bounded pool is correct and prevents a
// single worker from exhausting PostgreSQL connections when several replicas
// share one database. They are fixed internal defaults, not env knobs:
// PostgreSQL connection limits are an operator/deployment concern, and these
// values are deliberately conservative.
const (
	// DefaultDatabaseMaxOpenConns bounds each process's PostgreSQL connection
	// pool. Several Relay workers plus CLI commands may share one database, so
	// the per-process pool stays small.
	DefaultDatabaseMaxOpenConns = 8
	// DefaultDatabaseMaxIdleConns is the retained idle-connection count.
	DefaultDatabaseMaxIdleConns = 4
	// DefaultDatabaseConnMaxLifetime retires pooled connections periodically so
	// a database failover or restart is picked up without a process restart.
	DefaultDatabaseConnMaxLifetime = 30 * time.Minute
	// DefaultDatabaseConnMaxIdleTime closes connections idle in the pool.
	DefaultDatabaseConnMaxIdleTime = 5 * time.Minute
	// DefaultDatabaseConnectTimeout bounds the startup connectivity check.
	DefaultDatabaseConnectTimeout = 10 * time.Second
)

// Load reads the worker's configuration from the environment and returns a
// Config and the first configuration error, if any. Production calls it from
// worker.Run; administrative CLI commands use the narrow loaders below. A
// caller gets a Config value instead of reading environment variables directly.
//
// Every failure path RETURNS a clear, variable-naming error rather than logging
// and exiting, so Load never writes to a process boundary and never calls
// os.Exit: a missing required variable, an invalid NODE_ID, an
// invalid LOG_LEVEL, or an invalid positive integer/duration tuning knob is
// returned for worker startup to propagate. The process boundary — printing the
// error exactly once and choosing the exit code — stays in cmd/main.go.
//
// The required variables — the three REDIS_* values, DATABASE_URL, and NODE_ID
// — must be non-empty or Load returns an error (see requiredEnv); NODE_ID is
// additionally validated by ParseNodeID. NodeID is the worker's single
// stable node identity (the Redis consumer name, the relay.node_id
// container-ownership label, and the stats node key); there is NO hostname or
// generated fallback. InstanceID is always a fresh random "instance-<hex>" token
// unique to this process. The optional variables are read with os.Getenv and
// stay zero/empty when
// unset: the metrics and git-webhook addresses (METRICS_ADDR, GIT_WEBHOOK_ADDR)
// stay "" (no HTTP server), as do the Traefik routing fields and the NETWORKS
// list, preserving their opt-in semantics through the caller's non-empty
// guards. The retention windows are the exception to
// the "zero when unset" rule: REDIS_STREAM_RETENTION defaults to
// DefaultStreamRetention (24h), DLQ_RETENTION to DefaultDLQRetention (7
// days), REDIS_INVOCATION_RETENTION to DefaultInvocationRetention (48h), and
// STATS_RETENTION to DefaultStatsRetention (7 days) when
// UNSET, while an explicitly empty value disables the respective retention (0).
// MAX_CONCURRENT_INVOCATIONS and MAX_CONCURRENT_BUILDS default to 8 and 2, and
// MAX_BUFFERED_EVENTS to 16 (see ParsePositiveInt); MAX_EVENT_BYTES
// defaults to 256KiB with a hard ceiling of 1MiB and accepts a human-readable
// binary size or a bare integer byte count (see ParseMaxEventBytes);
// MAX_WARM_CONTAINERS defaults to 8; an invalid (zero, negative, non-integer,
// or above-limit) value is a returned configuration error. The retention
// windows instead log-and-disable rather than failing startup.
func Load(logger *slog.Logger) (Config, error) {
	var cfg Config

	uri, err := requiredEnv("REDIS_URI")
	if err != nil {
		return Config{}, err
	}
	cfg.RedisURI = uri

	stream, err := requiredEnv("REDIS_STREAM")
	if err != nil {
		return Config{}, err
	}
	cfg.RedisStream = stream

	group, err := requiredEnv("REDIS_GROUP")
	if err != nil {
		return Config{}, err
	}
	cfg.RedisGroup = group

	// The state database DSN is required: PostgreSQL backs the operator state
	// view, the per-node stats snapshots, and the durable schedule outbox. A
	// missing value is a startup error naming the variable, never a silent
	// fallback.
	databaseURL, err := requiredEnv("DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	cfg.DatabaseURL = databaseURL

	// NODE_ID is REQUIRED: it is the node's single stable identity — the
	// Redis consumer name, the relay.node_id container-ownership label, and the
	// stats node key. A missing value is a startup error naming the variable; a
	// malformed one is rejected by ParseNodeID. There is no
	// hostname or generated fallback.
	nodeID, err := requiredEnv("NODE_ID")
	if err != nil {
		return Config{}, err
	}
	nodeID, err = ParseNodeID(nodeID)
	if err != nil {
		return Config{}, err
	}
	cfg.NodeID = nodeID

	// InstanceID is ALWAYS fresh so a stale process incarnation of the same
	// NodeID can never overwrite a newer one's stats snapshots.
	instanceID, err := newIdentityToken(instanceIDPrefix)
	if err != nil {
		return Config{}, err
	}
	cfg.InstanceID = instanceID

	// Retention windows degrade rather than fail startup: a bad value logs and
	// disables the respective retention. The unset-vs-empty distinction is
	// deliberate — UNSET applies the documented default, while an explicitly
	// empty value keeps the historical opt-out (disabled).
	cfg.StreamRetention = retentionEnv(logger, "REDIS_STREAM_RETENTION", DefaultStreamRetention)
	cfg.DLQRetention = retentionEnv(logger, "DLQ_RETENTION", DefaultDLQRetention)
	cfg.InvocationRetention = retentionEnv(logger, "REDIS_INVOCATION_RETENTION", DefaultInvocationRetention)
	cfg.StatsRetention = retentionEnv(logger, "STATS_RETENTION", DefaultStatsRetention)
	cfg.MetricsAddr = getEnv("METRICS_ADDR", "")
	cfg.GitWebhookAddr = getEnv("GIT_WEBHOOK_ADDR", "")

	if _, err := ParseLogLevel(getEnv("LOG_LEVEL", "INFO")); err != nil {
		return Config{}, err
	}

	maxInvocations, err := ParsePositiveInt(
		"MAX_CONCURRENT_INVOCATIONS",
		getEnv("MAX_CONCURRENT_INVOCATIONS", strconv.Itoa(DefaultMaxConcurrentInvocations)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxConcurrentInvocations = maxInvocations

	maxBuilds, err := ParsePositiveInt(
		"MAX_CONCURRENT_BUILDS",
		getEnv("MAX_CONCURRENT_BUILDS", strconv.Itoa(DefaultMaxConcurrentBuilds)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxConcurrentBuilds = maxBuilds

	maxWarm, err := ParsePositiveInt(
		"MAX_WARM_CONTAINERS",
		getEnv("MAX_WARM_CONTAINERS", strconv.Itoa(DefaultMaxWarmContainers)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxWarmContainers = maxWarm

	maxBufferedEvents, err := ParsePositiveInt(
		"MAX_BUFFERED_EVENTS",
		getEnv("MAX_BUFFERED_EVENTS", strconv.Itoa(DefaultMaxBufferedEvents)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxBufferedEvents = maxBufferedEvents

	maxEventBytes, err := ParseMaxEventBytes(
		"MAX_EVENT_BYTES",
		getEnv("MAX_EVENT_BYTES", bytesize.Format(DefaultMaxEventBytes)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxEventBytes = maxEventBytes

	cfg.Networks = ParseNetworks(getEnv("NETWORKS", ""))
	cfg.TraefikNetwork = getEnv("TRAEFIK_NETWORK", "")
	cfg.TraefikEntryPoints = getEnv("TRAEFIK_ENTRYPOINTS", "")
	cfg.TraefikCertResolver = getEnv("TRAEFIK_CERTRESOLVER", "")

	priority, err := ParseOptionalPositiveInt("TRAEFIK_PRIORITY", getEnv("TRAEFIK_PRIORITY", ""))
	if err != nil {
		return Config{}, err
	}
	cfg.TraefikPriority = priority

	cfg.TraefikHostOverride = getEnv("TRAEFIK_HOST_OVERRIDE", "")

	warmTimeout, err := ParsePositiveDuration(
		"WARM_CONTAINER_IDLE_TIMEOUT",
		getEnv("WARM_CONTAINER_IDLE_TIMEOUT", DefaultWarmContainerIdleTimeout.String()),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.WarmContainerIdleTimeout = warmTimeout

	hotReload, err := ParseBool("HOT_RELOAD", getEnv("HOT_RELOAD", "false"))
	if err != nil {
		return Config{}, err
	}
	cfg.HotReload = hotReload

	return cfg, nil
}

// LoadDatabaseURL resolves just the state-database DSN from the environment.
// It exists for the administrative commands (`app ls`, `app inspect`, `stats`,
// `stats reset`) that need PostgreSQL but must work with no REDIS_URI,
// REDIS_STREAM, REDIS_GROUP, or Docker configured: those commands read/write the
// operator state view only. A missing DATABASE_URL returns a clear
// variable-naming error rather than a zero DSN, so an unconfigured CLI reports
// the real cause instead of a connection failure.
func LoadDatabaseURL() (string, error) {
	return requiredEnv("DATABASE_URL")
}

// ParseBool parses a boolean environment value using strconv.ParseBool's
// accepted forms (1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False);
// surrounding whitespace is trimmed first. Callers resolve any default at the
// getEnv call site. An empty or otherwise unparseable value is an error naming
// the variable, so a typo fatally fails startup rather than silently flipping a
// behavior (unlike the log-and-disable retention windows).
func ParseBool(name, value string) (bool, error) {
	v := strings.TrimSpace(value)
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: must be a boolean", name, value)
	}
	return b, nil
}

// ParseMaxEventBytes parses the MAX_EVENT_BYTES value: the maximum byte length
// of a message's raw `event` field. Callers resolve the default at the getEnv
// call site. It accepts a human-readable binary size (B, KiB, MiB, or GiB; e.g.
// "256KiB", "1MiB") or a bare positive integer byte count for backwards
// compatibility (e.g. "262144"), up to MaxEventBytesLimit (surrounding
// whitespace trimmed). It rejects empty, non-numeric, float, zero, negative,
// decimal units (KB/MB/GB), overflow, and above-limit values; the error names
// the variable, the required form, and the hard maximum. Zero is never read as
// "unlimited": the cap must always be a real bound. It is the ceiling analogue
// of ParsePositiveInt, so a typo fails startup instead of silently removing the
// oversized-message guard.
func ParseMaxEventBytes(name, value string) (int, error) {
	n, err := bytesize.ParseSize(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %v (hard maximum %s)", name, value, err, bytesize.Format(MaxEventBytesLimit))
	}
	if n > int64(MaxEventBytesLimit) {
		return 0, fmt.Errorf("invalid %s %q: must be no greater than %s", name, value, bytesize.Format(MaxEventBytesLimit))
	}
	return int(n), nil
}

// ParsePositiveInt parses a positive-integer environment value. Callers resolve
// any default at the getEnv call site, so the value here is the provided one. It
// accepts any parseable positive integer (surrounding whitespace is trimmed) and
// rejects empty, non-numeric, float, negative, zero, and overflow values. The
// error names the variable and the required form so callers (Load and tests)
// render a clear configuration error.
func ParsePositiveInt(name, value string) (int, error) {
	v := strings.TrimSpace(value)
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer", name, value)
	}
	if n <= 0 {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer", name, value)
	}
	return n, nil
}

// ParsePositiveDuration parses a positive Go duration environment value.
// Callers resolve any default at the getEnv call site, so the value here is the
// provided one. It must parse via time.ParseDuration AND be strictly positive
// (e.g. "5m", "90s", "1h30m"); empty, zero, negative, and malformed values are
// errors naming the variable. It is the duration analogue of ParsePositiveInt,
// used for WARM_CONTAINER_IDLE_TIMEOUT so a typo fails startup instead of
// silently disabling container eviction.
func ParsePositiveDuration(name, value string) (time.Duration, error) {
	v := strings.TrimSpace(value)
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: must be a positive duration", name, value)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid %s %q: must be a positive duration", name, value)
	}
	return d, nil
}

// ParseNetworks parses the NETWORKS value: a comma-separated list of Docker
// network names. Each entry is trimmed of surrounding whitespace, empty entries
// are ignored, duplicates are removed, and the DECLARATION order is preserved
// (unlike the former template-level normalization, which sorted). An unset or
// all-empty value yields nil, so "no networks" is the nil slice. It never
// fails: a network name is an operator-supplied Docker identifier, and an
// invalid one surfaces as a startup verification failure rather than a parse
// error. The value is startup configuration, parsed once by Load.
func ParseNetworks(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	for _, part := range strings.Split(value, ",") {
		name := strings.TrimSpace(part)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

// ParseOptionalPositiveInt parses an optional positive-integer environment
// value where absence is meaningful: unset/empty/whitespace-only returns
// (nil, nil) — the pointer-nil "not configured" state — while any provided
// value must parse as a positive integer (delegating to ParsePositiveInt,
// whose error names the variable). A nil return is what distinguishes an unset
// TRAEFIK_PRIORITY style value from &0: Relay never allows 0, but only
// nil-vs-non-nil can express "no value provided" through a *int config field.
func ParseOptionalPositiveInt(name, value string) (*int, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	n, err := ParsePositiveInt(name, value)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

// ParseNodeID validates a NODE_ID value into the stable node identity.
// NODE_ID is REQUIRED: callers resolve it with requiredEnv before validating,
// so an empty (or whitespace-only) value is an error rather than a generated
// fallback. The value is trimmed, must be at most maxNodeIDLen bytes, and may
// contain only ASCII letters, digits, '.', '_' and '-'. Any other value is an
// error naming NODE_ID (never silently coerced), so a missing or typo'd value
// fails startup rather than producing an unexpected consumer name or stats node key.
func ParseNodeID(value string) (string, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return "", fmt.Errorf("invalid NODE_ID %q: must not be empty", value)
	}
	if len(v) > maxNodeIDLen {
		return "", fmt.Errorf("invalid NODE_ID %q: must be at most %d characters", value, maxNodeIDLen)
	}
	for _, r := range v {
		if !isNodeIDChar(r) {
			return "", fmt.Errorf("invalid NODE_ID %q: only letters, digits, '.', '_' and '-' are allowed", value)
		}
	}
	return v, nil
}

// isNodeIDChar reports whether r is a permitted NODE_ID character. The set
// is deliberately conservative (ASCII letters, digits, '.', '_', '-'): the value
// becomes a stats row key and appears in logs, so whitespace-bearing, ambiguous,
// or non-ASCII values are rejected.
func isNodeIDChar(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r == '.', r == '_', r == '-':
		return true
	default:
		return false
	}
}

// newIdentityToken returns prefix followed by 8 bytes of crypto/rand rendered as
// lowercase hex (e.g. "instance-1f3a..."). A rand failure is returned rather than
// falling back to a predictable value: an identity that silently repeats could
// alias another process's stats row.
func newIdentityToken(prefix string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate %s identity: %w", strings.TrimSuffix(prefix, "-"), err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}

// logLevelNames are the documented LOG_LEVEL values, in order of increasing
// verbosity, used to render a helpful error message on an invalid value.
var logLevelNames = []string{"DEBUG", "INFO", "WARN", "ERROR"}

// ParseLogLevel parses a LOG_LEVEL value into a slog.Level. It trims leading
// and trailing whitespace and is case-insensitive ("info"/"Info"/"INFO" all
// work), though the documented form is uppercase. The accepted values map
// 1:1 onto slog's built-in levels: DEBUG, INFO, WARN and ERROR. "WARNING" is
// NOT accepted — it is treated as an invalid value, not an alias for WARN. An
// empty value returns slog.LevelInfo (the default). Any other value returns an
// error listing the valid values so callers can render a clear configuration
// error rather than silently falling back.
func ParseLogLevel(value string) (slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "":
		return slog.LevelInfo, nil
	case "DEBUG":
		return slog.LevelDebug, nil
	case "INFO":
		return slog.LevelInfo, nil
	case "WARN":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf(
			"invalid LOG_LEVEL %q: valid values are %s",
			value, strings.Join(logLevelNames, ", "),
		)
	}
}

// retentionEnv resolves a retention window from the environment variable name.
// Unlike getEnv it distinguishes UNSET from explicitly empty: UNSET returns
// defaultWindow (the documented default), while an explicitly set value — empty
// included — is parsed by parseRetention, where empty/zero/negative disables
// retention (0). This preserves the historical opt-out: setting the variable to
// an empty string still turns retention off, while leaving it unset keeps the
// default on by default.
func retentionEnv(logger *slog.Logger, name string, defaultWindow time.Duration) time.Duration {
	value, ok := os.LookupEnv(name)
	if !ok {
		return defaultWindow
	}
	return parseRetention(logger, name, value)
}

// parseRetention parses a stream-retention window from the environment variable
// name (passed as value). It returns 0 when value is empty or whitespace-only,
// which disables retention entirely (no goroutine, no trims). A malformed
// duration or a zero/negative value is logged (naming the variable) and
// retention is disabled (0): a bad retention value therefore logs and disables
// rather than failing startup. This is deliberate — a typo in one optional
// variable must not take down the worker; the operator sees the log line and
// the disabled behavior. The value carries no credentials, so echoing it in the
// log line is safe. The same helper serves REDIS_STREAM_RETENTION,
// DLQ_RETENTION, REDIS_INVOCATION_RETENTION, and STATS_RETENTION; only the
// caller's unset default differs.
func parseRetention(logger *slog.Logger, name, value string) time.Duration {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		logger.Warn("invalid retention window; retention disabled", "variable", name, "value", value, "error", err)
		return 0
	}
	if d <= 0 {
		logger.Warn("retention window must be positive; retention disabled", "variable", name, "value", value)
		return 0
	}
	return d
}

// requiredEnv returns the value of the environment variable key, or a clear
// configuration error if it is empty. It returns rather than exiting so the
// caller (worker startup or a narrow loader) propagates the failure to
// cmd/main.go, which owns error printing and process exit.
func requiredEnv(key string) (string, error) {
	if value := os.Getenv(key); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("required environment variable %s is not set", key)
}

// getEnv returns the value of the environment variable key, or defaultValue if
// it is empty.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

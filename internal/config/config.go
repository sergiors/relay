package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"relay/internal/bytesize"
)

// Config holds the application settings Relay's runtime (see internal/worker)
// and its supporting commands derive from the environment. Every field maps to
// a single environment variable; there are no computed or invented settings.
// Load() is the only constructor — application code should consume a Config
// value instead of reading environment variables directly.
//
// The required fields name the Redis stream, group, and address the worker
// consumes; ConsumerName is the resolved hostname identity (see consumerNameFromHost);
// and the address values (MetricsAddr, GitWebhookAddr) are empty when disabled,
// so gating on non-empty keeps them opt-in.
type Config struct {
	RedisURI     string
	RedisStream  string
	RedisGroup   string
	ConsumerName string
	// StreamRetention is the source-stream retention window
	// (REDIS_STREAM_RETENTION). Unset defaults to DefaultStreamRetention
	// (24h); an explicitly empty, zero, or negative value disables main-stream
	// retention (0), and a malformed value logs and disables it. The worker
	// trims the configured source stream with XTRIM ... ACKED, so entries are
	// removed only once every consumer group has acknowledged them.
	StreamRetention time.Duration
	// DLQRetention is the dead-letter-queue retention window
	// (REDIS_DLQ_RETENTION). Unset defaults to DefaultDLQRetention (7 days);
	// an explicitly empty, zero, or negative value disables DLQ retention (0),
	// and a malformed value logs and disables it. Unlike StreamRetention this
	// is AGE-ONLY: the worker trims the Relay-owned DLQ stream with
	// XTRIM ... MINID ~ (no ACKED mode), because the DLQ has no consumer group,
	// PEL, or XACK flow to protect.
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
	MetricsAddr         string
	// GitWebhookAddr is the address for the GitHub webhook server
	// (POST /github) that triggers an automatic git sync on matching pushes.
	// Like MetricsAddr it is opt-in via the GIT_WEBHOOK_ADDR environment
	// variable: empty (unset) disables the webhook server entirely, and a
	// non-empty value makes the worker bind it at startup (a bad address is a
	// fatal startup error, matching metrics). Sync itself remains a separate
	// manual command; the webhook server only schedules syncs through the
	// coalescing trigger in internal/git/webhook, never directly.
	GitWebhookAddr string
	// LogLevel is the slog level selected by LOG_LEVEL (default Info). It is
	// used by cmd/main.go to build the process logger after config.Load.
	LogLevel slog.Level
	// MaxConcurrentInvocations is the MAX_CONCURRENT_INVOCATIONS value (default
	// 8): the total number of app invocations executing concurrently in this
	// single Relay worker. Values below the default fall back in the runner (see
	// runner.SetMaxConcurrentInvocations); it is always positive after Load.
	MaxConcurrentInvocations int
	// MaxConcurrentBuilds is the MAX_CONCURRENT_BUILDS value (default 2): the
	// cap on runtime-backed image-preparation pipelines running concurrently in
	// this single Relay worker. One preparation pipeline (source selection and
	// snapshot, dependency snapshot, dependency image build, app image build)
	// occupies one slot for its WHOLE duration, so a preparation's dependency
	// and app sub-builds are sequential within one permit. It is INDEPENDENT of
	// MaxConcurrentInvocations and of MAX_BUFFERED_EVENTS, and it is not a
	// host-wide resource quota: each worker enforces its own, so a deployment of
	// N workers can run up to N * MaxConcurrentBuilds preparations. It is always
	// positive after Load.
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
	// DefaultDLQRetention is the dead-letter-queue retention window applied when
	// REDIS_DLQ_RETENTION is UNSET; an explicitly empty value disables it. The
	// DLQ has no consumer group, so its trim is AGE-ONLY (MINID ~, no ACKED).
	// Seven days mirrors the schedule outbox's retention, so a dead-lettered
	// entry outlives the schedule occurrence's SQLite-outbox window. It is
	// INDEPENDENT of the invocation-state terminal TTL below: a DLQ entry is
	// stream data, a terminal invocation-state hash is a per-message Redis hash.
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
)

// Load reads Relay's configuration from the environment and returns a Config and
// the first configuration error, if any. It is the single entry point for
// application configuration: callers get a Config value instead of reading
// environment variables directly.
//
// Every failure path RETURNS a clear, variable-naming error rather than logging
// and exiting, so Load never writes to a process boundary and never calls
// os.Exit: a missing required REDIS_* variable, an unresolvable hostname, an
// invalid LOG_LEVEL, or an invalid positive integer/duration tuning knob is
// returned for the caller (the CLI command or worker startup) to propagate. The
// process boundary — printing the error exactly once and choosing the exit code
// — stays in cmd/main.go.
//
// The required REDIS_* variables must be non-empty or Load returns an error (see
// requiredEnv); the consumer name is hostname-resolved via consumerNameFromHost.
// The optional variables are read with os.Getenv and stay zero/empty when
// unset: the metrics and git-webhook addresses (METRICS_ADDR, GIT_WEBHOOK_ADDR)
// stay "" (no HTTP server), preserving their opt-in semantics through the
// caller's non-empty guards. The three retention windows are the exception to
// the "zero when unset" rule: REDIS_STREAM_RETENTION defaults to
// DefaultStreamRetention (24h), REDIS_DLQ_RETENTION to DefaultDLQRetention (7
// days), and REDIS_INVOCATION_RETENTION to DefaultInvocationRetention (48h) when
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

	consumer, err := consumerNameFromHost()
	if err != nil {
		return Config{}, err
	}
	cfg.ConsumerName = consumer

	// Retention windows degrade rather than fail startup: a bad value logs and
	// disables the respective retention. The unset-vs-empty distinction is
	// deliberate — UNSET applies the documented default, while an explicitly
	// empty value keeps the historical opt-out (disabled).
	cfg.StreamRetention = retentionEnv(logger, "REDIS_STREAM_RETENTION", DefaultStreamRetention)
	cfg.DLQRetention = retentionEnv(logger, "REDIS_DLQ_RETENTION", DefaultDLQRetention)
	cfg.InvocationRetention = retentionEnv(logger, "REDIS_INVOCATION_RETENTION", DefaultInvocationRetention)
	cfg.MetricsAddr = getEnv("METRICS_ADDR", "")
	cfg.GitWebhookAddr = getEnv("GIT_WEBHOOK_ADDR", "")

	level, err := loadLogLevel(getEnv("LOG_LEVEL", "INFO"))
	if err != nil {
		return Config{}, err
	}
	cfg.LogLevel = level

	maxInvocations, err := loadPositiveInt(
		"MAX_CONCURRENT_INVOCATIONS",
		getEnv("MAX_CONCURRENT_INVOCATIONS", strconv.Itoa(DefaultMaxConcurrentInvocations)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxConcurrentInvocations = maxInvocations

	maxBuilds, err := loadPositiveInt(
		"MAX_CONCURRENT_BUILDS",
		getEnv("MAX_CONCURRENT_BUILDS", strconv.Itoa(DefaultMaxConcurrentBuilds)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxConcurrentBuilds = maxBuilds

	maxWarm, err := loadPositiveInt(
		"MAX_WARM_CONTAINERS",
		getEnv("MAX_WARM_CONTAINERS", strconv.Itoa(DefaultMaxWarmContainers)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxWarmContainers = maxWarm

	maxBufferedEvents, err := loadPositiveInt(
		"MAX_BUFFERED_EVENTS",
		getEnv("MAX_BUFFERED_EVENTS", strconv.Itoa(DefaultMaxBufferedEvents)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxBufferedEvents = maxBufferedEvents

	maxEventBytes, err := loadMaxEventBytes(getEnv("MAX_EVENT_BYTES", bytesize.Format(DefaultMaxEventBytes)))
	if err != nil {
		return Config{}, err
	}
	cfg.MaxEventBytes = maxEventBytes

	cfg.Networks = ParseNetworks(getEnv("NETWORKS", ""))
	cfg.TraefikNetwork = getEnv("TRAEFIK_NETWORK", "")
	cfg.TraefikEntryPoints = getEnv("TRAEFIK_ENTRYPOINTS", "")
	cfg.TraefikCertResolver = getEnv("TRAEFIK_CERTRESOLVER", "")

	priority, err := loadOptionalPositiveInt("TRAEFIK_PRIORITY", getEnv("TRAEFIK_PRIORITY", ""))
	if err != nil {
		return Config{}, err
	}
	cfg.TraefikPriority = priority

	cfg.TraefikHostOverride = getEnv("TRAEFIK_HOST_OVERRIDE", "")

	warmTimeout, err := loadPositiveDuration(
		"WARM_CONTAINER_IDLE_TIMEOUT",
		getEnv("WARM_CONTAINER_IDLE_TIMEOUT", DefaultWarmContainerIdleTimeout.String()),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.WarmContainerIdleTimeout = warmTimeout

	return cfg, nil
}

// loadPositiveInt parses a positive-integer environment value. Defaults are
// resolved at the getEnv call site (the env value is already non-empty), so an
// unparseable, zero, or negative value is a returned configuration error naming
// the variable (see ParsePositiveInt).
func loadPositiveInt(name, value string) (int, error) {
	return ParsePositiveInt(name, value)
}

// loadPositiveDuration parses a positive-duration environment value. Defaults
// are resolved at the getEnv call site, so an invalid (malformed or
// non-positive) value is a returned configuration error naming the variable
// (see ParsePositiveDuration).
func loadPositiveDuration(name, value string) (time.Duration, error) {
	return ParsePositiveDuration(name, value)
}

// loadMaxEventBytes parses MAX_EVENT_BYTES. Defaults are resolved at the
// getEnv call site (the value is always non-empty here), so an invalid value is
// a returned configuration error naming the variable (see ParseMaxEventBytes).
func loadMaxEventBytes(value string) (int, error) {
	return ParseMaxEventBytes("MAX_EVENT_BYTES", value)
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

// loadOptionalPositiveInt wraps ParseOptionalPositiveInt with Load's returned-
// error style: an invalid value returns a clear configuration error naming the
// variable. It returns nil for an unset value (see the parse helper for the
// nil-vs-provided contract).
func loadOptionalPositiveInt(name, value string) (*int, error) {
	return ParseOptionalPositiveInt(name, value)
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

// loadLogLevel parses LOG_LEVEL and returns a clear configuration error for an
// invalid value. Unlike optional tuning knobs (parseRetention falls back), an
// invalid log level is an error: silently running at an unintended level would
// obscure precisely the operational feedback the operator asked for. The valid
// value set is small and enumerated, so there is no ambiguity worth falling back
// on.
func loadLogLevel(value string) (slog.Level, error) {
	return ParseLogLevel(value)
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
// REDIS_DLQ_RETENTION, and REDIS_INVOCATION_RETENTION; only the caller's unset
// default differs.
func parseRetention(logger *slog.Logger, name, value string) time.Duration {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		logger.Warn("Redis: invalid retention window; retention disabled", "variable", name, "value", value, "error", err)
		return 0
	}
	if d <= 0 {
		logger.Warn("Redis: retention window must be positive; retention disabled", "variable", name, "value", value)
		return 0
	}
	return d
}

// consumerNameFromHost resolves the hostname into a consumer name. It returns a
// clear error on both a hostname resolution failure and an empty hostname,
// keeping the two failure paths distinct in their messages. os.Hostname is not
// injectable, so the failure paths cannot be exercised in-process; they are
// returned (not fatal) so Load's caller owns process exit.
func consumerNameFromHost() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve consumer name: hostname unavailable: %w", err)
	}
	if host == "" {
		return "", errors.New("resolve consumer name: hostname is empty")
	}
	return host, nil
}

// requiredEnv returns the value of the environment variable key, or a clear
// configuration error if it is empty. It returns rather than exiting so Load's
// caller (the CLI or worker startup) propagates the failure to cmd/main.go,
// which owns error printing and process exit.
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

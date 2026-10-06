package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application settings Relay's runtime (see internal/worker)
// and its supporting commands derive from the environment. Every field maps to
// a single environment variable; there are no computed or invented settings.
// Load() is the only constructor — application code should consume a Config
// value instead of reading environment variables directly.
//
// The required fields name the Redis stream, group, and address the worker
// consumes; ConsumerName is the resolved hostname identity (see consumerNameFromHost);
// and the two optional values (StreamRetention, MetricsAddr) are zero when
// disabled, so gating on non-zero / non-empty keeps them opt-in.
type Config struct {
	RedisURI        string
	RedisStream     string
	RedisGroup      string
	ConsumerName    string
	StreamRetention time.Duration
	MetricsAddr     string
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
	// MaxConcurrency is the MAX_CONCURRENCY value (default 8): the total number
	// of app invocations executing concurrently in this single Relay
	// worker. Values below the default fall back in the runner (see
	// runner.SetMaxConcurrency); it is always positive after Load.
	MaxConcurrency int
	// MaxBufferedEvents is the MAX_BUFFERED_EVENTS value (default 16): the
	// number of events already read from Redis and still held locally by this
	// worker before they complete/ACK. It bounds the local buffer so the
	// backlog stays in Redis when full. It is always positive after Load.
	MaxBufferedEvents int
	// MaxEventBytes is the MAX_EVENT_BYTES value (default 262144, hard max
	// 1048576): the maximum byte length of a message's raw `event` field. A
	// message whose raw event value exceeds it is rejected before JSON decode,
	// schedule classification, event matching, and handler execution, and is
	// routed to the DLQ with a bounded diagnostic summary instead of the
	// payload. It bounds the RAW event value only — not the whole Redis entry
	// or its RESP/response overhead, which Redis and go-redis have already
	// materialized before Relay can inspect it. It is always positive after
	// Load: zero is rejected, never read as "unlimited".
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
	// MAX_CONCURRENCY rather than REDIS_STREAM_RETENTION's log-and-disable
	// style (a typo in a container-warmth knob must not silently change runtime
	// behavior). It is always positive after Load.
	WarmContainerIdleTimeout time.Duration
}

// Default max-concurrency and max-buffered-events values. The runner and stream
// layers keep their own copies of these constants (a leaf package cannot import
// config); this package owns the env-facing defaults.
const (
	DefaultMaxConcurrency    = 8
	DefaultMaxBufferedEvents = 16
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
// unset: retention maps to 0 (disabled, see parseRetention) and the metrics
// and git-webhook addresses (METRICS_ADDR, GIT_WEBHOOK_ADDR) to "" (no HTTP
// server), preserving their opt-in semantics through the caller's non-zero /
// non-empty guards. MAX_CONCURRENCY and MAX_BUFFERED_EVENTS
// default to 8 and 16 respectively (see ParsePositiveInt); MAX_EVENT_BYTES
// defaults to 262144 with a hard ceiling of 1048576 (see ParseMaxEventBytes);
// an invalid (zero, negative, non-integer, or above-limit) value is a returned
// configuration error. The only value that still logs-and-disables rather than
// failing is the optional REDIS_STREAM_RETENTION window.
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

	// REDIS_STREAM_RETENTION is the one optional knob that degrades rather than
	// fails: parseRetention logs and returns 0 (disabled) for a bad value.
	cfg.StreamRetention = parseRetention(logger, getEnv("REDIS_STREAM_RETENTION", ""))
	cfg.MetricsAddr = getEnv("METRICS_ADDR", "")
	cfg.GitWebhookAddr = getEnv("GIT_WEBHOOK_ADDR", "")

	level, err := loadLogLevel(getEnv("LOG_LEVEL", "INFO"))
	if err != nil {
		return Config{}, err
	}
	cfg.LogLevel = level

	maxConcurrency, err := loadPositiveInt(
		"MAX_CONCURRENCY",
		getEnv("MAX_CONCURRENCY", strconv.Itoa(DefaultMaxConcurrency)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxConcurrency = maxConcurrency

	maxBufferedEvents, err := loadPositiveInt(
		"MAX_BUFFERED_EVENTS",
		getEnv("MAX_BUFFERED_EVENTS", strconv.Itoa(DefaultMaxBufferedEvents)),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxBufferedEvents = maxBufferedEvents

	maxEventBytes, err := loadMaxEventBytes(getEnv("MAX_EVENT_BYTES", strconv.Itoa(DefaultMaxEventBytes)))
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
// call site. It accepts any parseable positive integer up to MaxEventBytesLimit
// (surrounding whitespace trimmed) and rejects empty, non-numeric, float,
// zero, negative, overflow, and above-limit values; the error names the
// variable, the required form, and the hard maximum. Zero is never read as
// "unlimited": the cap must always be a real bound. It is the ceiling analogue
// of ParsePositiveInt, so a typo fails startup instead of silently removing the
// oversized-message guard.
func ParseMaxEventBytes(name, value string) (int, error) {
	v := strings.TrimSpace(value)
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer no greater than %d", name, value, MaxEventBytesLimit)
	}
	if n <= 0 {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer no greater than %d", name, value, MaxEventBytesLimit)
	}
	if n > MaxEventBytesLimit {
		return 0, fmt.Errorf("invalid %s %q: must be no greater than %d", name, value, MaxEventBytesLimit)
	}
	return n, nil
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

// parseRetention parses a stream-retention window from REDIS_STREAM_RETENTION
// (passed as value). It returns 0 when value is unset or empty, which disables
// stream retention entirely (no goroutine, no trims). A malformed duration or a
// zero/negative value is logged and retention is disabled (0): a bad
// REDIS_STREAM_RETENTION therefore logs and disables retention rather than
// failing startup. This is a deliberate change — a typo in one optional
// variable must not take down the worker; the operator sees the log line and
// the retained (disabled) behavior. The value carries no credentials, so
// echoing it in the log line is safe.
func parseRetention(logger *slog.Logger, value string) time.Duration {
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		logger.Warn("Redis: invalid REDIS_STREAM_RETENTION; retention disabled", "value", value, "error", err)
		return 0
	}
	if d <= 0 {
		logger.Warn("Redis: REDIS_STREAM_RETENTION must be positive; retention disabled", "value", value)
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

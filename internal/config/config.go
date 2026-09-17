package config

import (
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
	// of function invocations executing concurrently in this single Relay
	// worker. Values below the default fall back in the runner (see
	// runner.SetMaxConcurrency); it is always positive after Load.
	MaxConcurrency int
	// MaxBufferedEvents is the MAX_BUFFERED_EVENTS value (default 16): the
	// number of events already read from Redis and still held locally by this
	// worker before they complete/ACK. It bounds the local buffer so the
	// backlog stays in Redis when full. It is always positive after Load.
	MaxBufferedEvents int
	// TraefikNetwork is the optional TRAEFIK_NETWORK value: the Docker network
	// Traefik is attached to. It is required only when a function's template
	// service declares a `host` (routed service); Relay never creates the
	// network itself and verifies it exists on every routed reconcile. Empty
	// means routing is not configured (unrouted services are unaffected).
	TraefikNetwork string
}

// Load reads Relay's configuration from the environment and returns a Config. It
// is the single entry point for application configuration: callers get a Config
// value instead of reading environment variables directly. It only reaches the
// executable boundary (the CLI commands and the worker call Load at startup),
// so the failure paths are fatal rather than returned errors: a missing required
// REDIS_* variable, an unresolvable hostname, or an invalid LOG_LEVEL exits via
// os.Exit, each naming what went wrong.
//
// The required REDIS_* variables must be non-empty or the process exits (see
// requiredEnv); the consumer name is hostname-resolved via consumerNameFromHost.
// The optional variables are read with os.Getenv and stay zero/empty when
// unset: retention maps to 0 (disabled, see parseRetention) and the metrics
// and git-webhook addresses (METRICS_ADDR, GIT_WEBHOOK_ADDR) to "" (no HTTP
// server), preserving their opt-in semantics through the caller's non-zero /
// non-empty guards. MAX_CONCURRENCY and MAX_BUFFERED_EVENTS
// default to 8 and 16 respectively (see ParsePositiveInt); an invalid (zero,
// negative, or non-integer) value is a configuration error and aborts startup,
// matching the loadLogLevel style.
func Load(logger *slog.Logger) Config {
	return Config{
		RedisURI:          requiredEnv(logger, "REDIS_URI"),
		RedisStream:       requiredEnv(logger, "REDIS_STREAM"),
		RedisGroup:        requiredEnv(logger, "REDIS_GROUP"),
		ConsumerName:      consumerNameFromHost(logger),
		StreamRetention:   parseRetention(logger, getEnv("REDIS_STREAM_RETENTION", "")),
		MetricsAddr:       getEnv("METRICS_ADDR", ""),
		GitWebhookAddr:    getEnv("GIT_WEBHOOK_ADDR", ""),
		LogLevel:          loadLogLevel(logger, getEnv("LOG_LEVEL", "INFO")),
		MaxConcurrency:    loadPositiveInt(logger, "MAX_CONCURRENCY", getEnv("MAX_CONCURRENCY", ""), DefaultMaxConcurrency),
		MaxBufferedEvents: loadPositiveInt(logger, "MAX_BUFFERED_EVENTS", getEnv("MAX_BUFFERED_EVENTS", ""), DefaultMaxBufferedEvents),
		TraefikNetwork:    getEnv("TRAEFIK_NETWORK", ""),
	}

}

// Default max-concurrency and max-buffered-events values. The runner and stream
// layers keep their own copies of these constants (a leaf package cannot import
// config); this package owns the env-facing defaults.
const (
	DefaultMaxConcurrency    = 8
	DefaultMaxBufferedEvents = 16
)

// loadPositiveInt parses an optional positive-integer environment value,
// falling back to def when unset/empty. An unparseable, zero, or negative value
// is a configuration error: it logs and aborts startup, matching loadLogLevel.
// The injected logger is non-nil at this entry point (the CLI owns logger
// creation).
func loadPositiveInt(logger *slog.Logger, name, value string, def int) int {
	n, err := ParsePositiveInt(name, value, def)
	if err != nil {
		logger.Error("Configuration error", "error", err)
		os.Exit(1)
	}
	return n
}

// ParsePositiveInt parses a positive-integer environment value, returning def
// when value is unset/empty. It accepts any parseable positive integer
// (surrounding whitespace is trimmed) and rejects non-numeric, float, negative,
// zero, and overflow values. The error names the variable and the required
// form so callers (Load and tests) render a clear configuration error.
func ParsePositiveInt(name, value string, def int) (int, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer", name, value)
	}
	if n <= 0 {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer", name, value)
	}
	return n, nil
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

// loadLogLevel parses LOG_LEVEL and, on an invalid value, reports a clear
// configuration error and aborts startup. Unlike optional tuning knobs
// (loadDuration falls back), an invalid log level is a configuration error:
// silently running at an unintended level would obscure precisely the
// operational feedback the operator asked for. The valid value set is small and
// enumerated, so there is no ambiguity worth falling back on. The injected
// logger is non-nil at this entry point (the CLI owns logger creation).
func loadLogLevel(logger *slog.Logger, value string) slog.Level {
	level, err := ParseLogLevel(value)
	if err != nil {
		logger.Error("Configuration error", "error", err)
		os.Exit(1)
	}
	return level
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
		logger.Warn(fmt.Sprintf("Redis: invalid REDIS_STREAM_RETENTION %q: %v", value, err))
		return 0
	}
	if d <= 0 {
		logger.Warn(fmt.Sprintf("Redis: REDIS_STREAM_RETENTION must be positive, got %q", value))
		return 0
	}
	return d
}

// consumerNameFromHost resolves the hostname into a consumer name. Failures are
// fatal: it logs and exits the process (os.Exit(1)) on both a hostname
// resolution error and an empty hostname, keeping the two failure paths
// distinct in their messages. It is only reached from the executable boundary's
// Load, so the fatal exit is intentional; it is wrapped here so the two failure
// paths stay distinguishable in the log. os.Hostname is not injectable, so
// these fatals are not unit-testable in-process.
func consumerNameFromHost(logger *slog.Logger) string {
	host, err := os.Hostname()
	if err != nil {
		logger.Error(fmt.Sprintf("Resolve consumer name: hostname unavailable: %v", err))
		os.Exit(1)
		return ""
	}
	if host == "" {
		logger.Error("Resolve consumer name: hostname is empty")
		os.Exit(1)
		return ""
	}
	return host
}

// requiredEnv returns the value of the environment variable key, or logs a
// clear configuration error and exits the process (os.Exit(1)) if it is empty.
//
// NOTE: this is PRE-EXISTING behavior (missing required variable = fatal)
// preserved through the slog migration: requiredEnv logs at Error and calls
// os.Exit(1) on a missing required environment variable. It is only ever
// reached from the executable boundary (the Relay CLI commands call Load), so
// the fatal exit is intentional and must not be converted to a returned error.
// The injected logger is non-nil at this entry point (the CLI owns logger
// creation).
func requiredEnv(logger *slog.Logger, key string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	logger.Error(fmt.Sprintf("Required environment variable %s is not set", key))
	os.Exit(1)
	return ""
}

// getEnv returns the value of the environment variable key, or defaultValue if
// it is empty.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

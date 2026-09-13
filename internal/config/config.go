package config

import (
	"log"
	"os"
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
}

// Load reads Relay's configuration from the environment and returns a Config. It
// is the single entry point for application configuration: callers get a Config
// value instead of reading environment variables directly. It only reaches the
// executable boundary (the CLI commands and the worker call Load at startup),
// so the failure paths are fatal rather than returned errors: a missing required
// REDIS_* variable or an unresolvable hostname exits via logger.Fatalf, both
// naming what went wrong.
//
// The required REDIS_* variables must be non-empty or the process exits via
// logger.Fatalf (see requiredEnv); the consumer name is hostname-resolved via
// consumerNameFromHost. The two optional variables are read with os.Getenv and
// stay zero/empty when unset: retention maps to 0 (disabled, see parseRetention)
// and the metrics address to "" (no HTTP server), preserving their opt-in
// semantics through the caller's non-zero / non-empty guards.
func Load(logger *log.Logger) Config {
	return Config{
		RedisURI:        requiredEnv(logger, "REDIS_URI"),
		RedisStream:     requiredEnv(logger, "REDIS_STREAM"),
		RedisGroup:      requiredEnv(logger, "REDIS_GROUP"),
		ConsumerName:    consumerNameFromHost(logger),
		StreamRetention: parseRetention(logger, getEnv("REDIS_STREAM_RETENTION", "")),
		MetricsAddr:     getEnv("METRICS_ADDR", ""),
	}

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
func parseRetention(logger *log.Logger, value string) time.Duration {
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		logger.Printf("Redis: invalid REDIS_STREAM_RETENTION %q: %v", value, err)
		return 0
	}
	if d <= 0 {
		logger.Printf("Redis: REDIS_STREAM_RETENTION must be positive, got %q", value)
		return 0
	}
	return d
}

// consumerNameFromHost resolves the hostname into a consumer name. Failures are
// fatal: it calls logger.Fatalf (process exit) on both a hostname resolution
// error and an empty hostname, keeping the two failure paths distinct in their
// messages. It is only reached from the executable boundary's Load (see the
// requiredEnv NOTE), so the fatal exit is intentional; it is wrapped here so
// the two failure paths stay distinguishable in the log. os.Hostname is not
// injectable, so these fatals are not unit-testable in-process.
func consumerNameFromHost(logger *log.Logger) string {
	host, err := os.Hostname()
	if err != nil {
		logger.Fatalf("Resolve consumer name: hostname unavailable: %v", err)
		return ""
	}
	if host == "" {
		logger.Fatalf("Resolve consumer name: hostname is empty")
		return ""
	}
	return host
}

// requiredEnv returns the value of the environment variable key, or exits the
// process if it is empty.
//
// NOTE: this is PRE-EXISTING behavior preserved as-is. requiredEnv calls
// logger.Fatalf (process exit) on a missing required environment variable. It
// is only ever reached from the executable boundary (the Relay CLI commands
// call Load), so the fatal exit is intentional and must not be converted to a
// returned error. The injected logger is non-nil at this entry point (the CLI
// owns logger creation).
func requiredEnv(logger *log.Logger, key string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	logger.Fatalf("Required environment variable %s is not set", key)
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

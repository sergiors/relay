// Package config resolves Relay's runtime settings from the environment at the
// executable boundary. Load is the single constructor: the worker and the CLI
// commands call it once at startup and pass the resulting Config value around,
// so application code never reads environment variables directly.
//
// Every Config field maps 1:1 to an environment variable, with no computed or
// invented settings. Required Redis variables (REDIS_URI, REDIS_STREAM,
// REDIS_GROUP) and malformed tuning knobs are RETURNED as errors from Load, not
// fatal: the caller propagates them to cmd/main.go, the sole process boundary
// that prints the error and owns os.Exit. The optional knobs (stream, DLQ, and
// invocation-state retention, metrics/webhook addresses, Traefik routing, and
// the NETWORKS list) stay zero/empty when unset and are gated by the caller;
// only the optional retention windows log-and-disable on a bad value instead of
// failing. The parse helpers (ParsePositiveInt, ParsePositiveDuration,
// ParseOptionalPositiveInt, ParseLogLevel, ParseMaxEventBytes, ParseNetworks)
// are exported and pure
// so they are directly testable; RedisOptions
// maps a Redis address or DSN to go-redis options without echoing credentials.
package config

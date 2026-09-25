// Package config resolves Relay's runtime settings from the environment at the
// executable boundary. Load is the single constructor: the worker and the CLI
// commands call it once at startup and pass the resulting Config value around,
// so application code never reads environment variables directly.
//
// Every Config field maps 1:1 to an environment variable, with no computed or
// invented settings. Required Redis variables (REDIS_URI, REDIS_STREAM,
// REDIS_GROUP) and malformed tuning knobs abort startup via os.Exit, while the
// optional knobs (stream retention, metrics/webhook addresses, Traefik routing,
// and the NETWORKS list) stay zero/empty when unset and are gated by the
// caller. The parse helpers (ParsePositiveInt, ParsePositiveDuration,
// ParseOptionalPositiveInt, ParseLogLevel, ParseNetworks) are exported and pure
// so they are directly testable; RedisOptions
// maps a Redis address or DSN to go-redis options without echoing credentials.
package config

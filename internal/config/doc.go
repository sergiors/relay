// Package config resolves Relay's runtime settings from the environment at the
// executable boundary. worker.Run calls Load once at worker startup and passes
// the resulting Config value around; the administrative CLI commands instead
// call the narrow LoadDatabaseURL loader for just the values they need.
// Application code never reads environment variables directly.
//
// Every Config field maps 1:1 to an environment variable, except the
// always-generated per-process InstanceID. The required worker variables and
// malformed non-retention tuning knobs are RETURNED as errors from Load, not
// fatal: the caller propagates them to
// cmd/main.go, the sole process boundary that prints the error and owns
// os.Exit. The optional addresses, Traefik routing, and the NETWORKS list stay
// zero/empty when unset. The retention windows default when unset
// (REDIS_STREAM_RETENTION 24h, DLQ_RETENTION 7d,
// REDIS_INVOCATION_RETENTION 48h, STATS_RETENTION 7d); an explicitly empty,
// zero, or negative value disables them, and a malformed value logs and
// disables instead of failing. The parse helpers (ParsePositiveInt,
// ParsePositiveDuration, ParseOptionalPositiveInt, ParseLogLevel,
// ParseMaxEventBytes, ParseNetworks) are exported and pure so they are directly
// testable; RedisOptions maps a Redis address or DSN to go-redis options without
// echoing credentials.
package config

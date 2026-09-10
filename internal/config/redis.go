package config

import (
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisOptions builds go-redis client options from the REDIS_ADDR environment
// variable. REDIS_ADDR accepts either a plain address ("host:port", e.g.
// "redis:6379") or a Redis DSN ("redis://user:password@host:port" or
// "rediss://..." for TLS). DSNs are parsed with redis.ParseURL; plain
// addresses keep the existing behavior (backward compatible).
//
// The value is read with MustEnv, so an unset or empty REDIS_ADDR panics with
// the established fail-fast message. On a malformed DSN the returned error is
// deliberately redacted: redis.ParseURL's own error text echoes the full URL,
// which would leak the password, so the error carries only a generic
// description and never the raw value.
func RedisOptions() (*redis.Options, error) {
	value := MustEnv("REDIS_ADDR")
	if strings.HasPrefix(value, "redis://") || strings.HasPrefix(value, "rediss://") {
		opts, err := redis.ParseURL(value)
		if err != nil {
			// Do not wrap err: its text includes the URL (and thus the
			// password). A generic message keeps credentials out of logs.
			return nil, errors.New("redis: invalid REDIS_ADDR DSN (credentials not shown)")
		}
		return opts, nil
	}
	return &redis.Options{Addr: value}, nil
}

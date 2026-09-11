package config

import (
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisOptions builds go-redis options from a plain address or Redis DSN.
func RedisOptions(addr string) (*redis.Options, error) {
	if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
		opts, err := redis.ParseURL(addr)
		if err != nil {
			// Do not wrap the parse error because it may contain credentials.
			return nil, errors.New("redis: invalid REDIS_ADDR DSN (credentials not shown)")
		}
		return opts, nil
	}
	return &redis.Options{Addr: addr}, nil
}

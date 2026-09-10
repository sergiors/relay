package config

import (
	"fmt"
	"os"
	"time"
)

// StreamRetention reads the optional REDIS_STREAM_RETENTION environment
// variable and returns the stream-retention window it configures. It returns
// (0, nil) when the variable is unset or empty, which disables stream retention
// entirely (no goroutine, no trims).
//
// Unlike the required REDIS_* variables, this one is OPTIONAL, so it is read
// with os.Getenv rather than MustEnv and never panics. A malformed duration or
// a zero/negative value is a configuration error returned to the caller (the
// worker surfaces it as a startup failure, matching how other config errors
// surface). The value carries no credentials, so echoing it in the error is
// safe.
func StreamRetention() (time.Duration, error) {
	value := os.Getenv("REDIS_STREAM_RETENTION")
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("redis: invalid REDIS_STREAM_RETENTION %q: %w", value, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("redis: REDIS_STREAM_RETENTION must be positive, got %q", value)
	}
	return d, nil
}

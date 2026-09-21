package testutil

import "os"

// EnvOr returns the value of the environment variable key, or fallback when it
// is empty or unset. It is the shared replacement for the per-package envOr
// copies used to read TEST-controlled variables such as REDIS_TEST_ADDR
// (config.getEnv is unexported, so packages cannot call it).
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

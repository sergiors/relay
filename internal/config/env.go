package config

import (
	"fmt"
	"os"
)

func MustEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		panic(fmt.Sprintf("missing required environment variable: %s", key))
	}
	return value
}

// Env returns the value of the environment variable key, or fallback when the
// variable is unset or empty. Unlike MustEnv it never panics, so it is used for
// optional configuration that has a sensible default.
func Env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

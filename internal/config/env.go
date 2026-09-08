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

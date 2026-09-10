//go:build integration

package cli

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
)

// healthRedisAddr is the Redis address used by the health integration test,
// overridable via REDIS_TEST_ADDR (mirrors internal/stream).
func healthRedisAddr() string {
	if v := os.Getenv("REDIS_TEST_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

// healthDockerAvailable reports whether the Docker daemon is reachable via the
// Engine API (mirrors internal/stream).
func healthDockerAvailable(t *testing.T) bool {
	t.Helper()
	if os.Getenv("RELAY_SKIP_DOCKER") != "" {
		return false
	}
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Logf("docker client: %v", err)
		return false
	}
	defer cli.Close()
	if _, err := cli.Ping(context.Background(), client.PingOptions{}); err != nil {
		t.Logf("docker unavailable: %v", err)
		return false
	}
	return true
}

// healthRedisAvailable reports whether a real Redis is reachable (mirrors
// internal/stream).
func healthRedisAvailable(t *testing.T) bool {
	t.Helper()
	if os.Getenv("RELAY_SKIP_REDIS") != "" {
		return false
	}
	cli := redis.NewClient(&redis.Options{Addr: healthRedisAddr()})
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Logf("redis unavailable: %v", err)
		return false
	}
	return true
}

// TestIntegrationHealthRealPath verifies the real `relay health` path exits 0
// when both Redis and Docker are reachable, and exits 1 when Redis is not.
func TestIntegrationHealthRealPath(t *testing.T) {
	if !healthRedisAvailable(t) {
		t.Skip("redis not available")
	}
	if !healthDockerAvailable(t) {
		t.Skip("docker not available")
	}

	// Point the command at the real test Redis.
	t.Setenv("REDIS_ADDR", healthRedisAddr())
	if code := runHealthCommand(); code != 0 {
		t.Fatalf("health with reachable redis+docker = %d, want 0", code)
	}

	// A dead Redis address must fail the redis check (exit 1).
	t.Setenv("REDIS_ADDR", "127.0.0.1:1")
	if code := runHealthCommand(); code != 1 {
		t.Fatalf("health with unreachable redis = %d, want 1", code)
	}
}

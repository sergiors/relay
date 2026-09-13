//go:build integration

// This file exercises the `relay health` command's real path against reachable
// Redis and Docker.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES both a reachable
// Docker daemon AND Redis at REDIS_TEST_ADDR (default localhost:6379, matching
// compose.dev.yaml); a missing dependency fails the affected tests rather than
// skipping them. Start the documented dev dependencies with
// `docker compose -f compose.dev.yaml up -d`. The Docker daemon is located via
// client.FromEnv, so DOCKER_HOST, the local socket, and a socket proxy are all
// respected.
package cli

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"

	"relay/internal/config"
)

// envOr returns the value of the environment variable key, or fallback when it
// is unset or empty. It replaces the old config.Env helper (removed when the
// config package dropped its generic env helpers) for reading TEST-controlled
// variables; required application settings come from config.Load.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// requireRedis fails the test when the test Redis (REDIS_TEST_ADDR, default
// localhost:6379) is not reachable, instead of skipping: the health command is
// meaningless without it.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := envOr("REDIS_TEST_ADDR", "localhost:6379")
	opts, _ := config.RedisOptions(addr)
	cli := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		t.Fatalf(
			"redis integration test requires a reachable Redis at %s (ping: %v); start one with `docker compose -f compose.dev.yaml up -d`",
			addr,
			err,
		)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// requireDocker fails the test immediately when the Docker Engine API daemon
// cannot be reached via client.FromEnv (DOCKER_HOST, socket, socket proxy are
// all respected). The health command checks Docker reachability; missing
// infrastructure fails rather than skips.
func requireDocker(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker integration test requires a Docker daemon (client: %v)", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Fatalf("docker integration test requires a reachable Docker daemon (ping: %v); start one or run `docker compose -f compose.dev.yaml up -d`", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// TestIntegrationHealthRealPath verifies the real `relay health` path exits 0
// when both Redis and Docker are reachable, and exits 1 when Redis is not.
func TestIntegrationHealthRealPath(t *testing.T) {
	requireRedis(t)
	requireDocker(t)

	// The health command loads full config via config.Load, which exits via
	// logger.Fatalf when a required REDIS_* variable is missing, so provide a
	// discard logger and set all three required variables (REDIS_STREAM and
	// REDIS_GROUP need only be non-empty; only REDIS_URI is pinged).
	logger := log.New(io.Discard, "", 0)
	redisURI := envOr("REDIS_TEST_ADDR", "localhost:6379")
	t.Setenv("REDIS_URI", redisURI)
	t.Setenv("REDIS_STREAM", "health-itest-stream")
	t.Setenv("REDIS_GROUP", "health-itest-group")

	// Point the command at the test Redis.
	var writer bytes.Buffer
	if err := runHealthCommand(context.Background(), &writer, logger); err != nil {
		t.Fatalf("health with reachable redis+docker failed: %v", err)
	}

	// A dead Redis address must fail the redis check.
	t.Setenv("REDIS_URI", "127.0.0.1:1")
	if err := runHealthCommand(context.Background(), &writer, logger); err == nil {
		t.Fatal("health with unreachable redis should have failed")
	}
}

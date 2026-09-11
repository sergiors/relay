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
	"context"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"

	"relay/internal/config"
)

// requireRedis fails the test when the test Redis (REDIS_TEST_ADDR, default
// localhost:6379) is not reachable, instead of skipping: the health command is
// meaningless without it.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := config.Env("REDIS_TEST_ADDR", "localhost:6379")
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

	// Point the command at the real test Redis.
	t.Setenv("REDIS_ADDR", config.Env("REDIS_TEST_ADDR", "localhost:6379"))
	if code := runHealthCommand(); code != 0 {
		t.Fatalf("health with reachable redis+docker = %d, want 0", code)
	}

	// A dead Redis address must fail the redis check (exit 1).
	t.Setenv("REDIS_ADDR", "127.0.0.1:1")
	if code := runHealthCommand(); code != 1 {
		t.Fatalf("health with unreachable redis = %d, want 1", code)
	}
}

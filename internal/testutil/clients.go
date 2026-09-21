package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"

	"relay/internal/config"
)

// RequireRedis fails the test when the test Redis (REDIS_TEST_ADDR, default
// localhost:6379) is not reachable, instead of skipping: the integration suites
// that use it are meaningless without real Redis state. The client is closed via
// t.Cleanup.
func RequireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	opts, _ := config.RedisOptions(addr)
	cli := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		t.Fatalf(
			"redis integration test requires a reachable Redis at %s (ping: %v); "+
				"start one with `docker compose -f compose.dev.yaml up -d`",
			addr,
			err,
		)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// RequireDocker fails the test immediately when the Docker Engine API daemon
// cannot be reached via client.FromEnv (DOCKER_HOST, the socket, and a socket
// proxy are all respected). Integration tests fundamentally require Docker;
// missing infrastructure fails rather than skips. The client is closed via
// t.Cleanup.
func RequireDocker(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker integration test requires a Docker daemon (client: %v)", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Fatalf(
			"docker integration test requires a reachable Docker daemon (ping: %v); "+
				"start one or run `docker compose -f compose.dev.yaml up -d`",
			err,
		)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

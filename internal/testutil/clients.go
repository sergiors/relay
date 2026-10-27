package testutil

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"

	"relay/internal/config"
)

// RequireRedis fails the test when the test Redis (REDIS_TEST_ADDR, default
// localhost:6379) is not reachable, instead of skipping: the integration suites
// that use it are meaningless without real Redis state. REDIS_TEST_ADDR may be
// a host:port address or a redis:// / rediss:// URL; diagnostics name only the
// server, never URL credentials. The client is closed via t.Cleanup.
func RequireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	opts, err := config.RedisOptions(addr)
	if err != nil {
		t.Fatalf(
			"redis integration test requires a valid Redis address at %s "+
				"(from REDIS_TEST_ADDR): %v",
			redisEndpoint(addr),
			err,
		)
	}
	cli := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		t.Fatalf(
			"redis integration test requires a reachable Redis at %s "+
				"(from REDIS_TEST_ADDR); start Redis and point "+
				"REDIS_TEST_ADDR at it (ping: %s)",
			redisEndpoint(addr),
			redactEndpointCredentials(err.Error()),
		)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// redisEndpoint names the configured Redis server without URL credentials. The
// documented form is a plain host:port, which is shown as-is.
func redisEndpoint(addr string) string {
	if !strings.HasPrefix(addr, "redis://") &&
		!strings.HasPrefix(addr, "rediss://") {
		// A malformed plain address with userinfo would otherwise put the
		// credentials in the failure message; it is not a supported form.
		if strings.Contains(addr, "@") {
			return "the configured Redis server"
		}
		return addr
	}
	u, err := url.Parse(addr)
	if err != nil || u.Host == "" {
		return "the configured Redis server"
	}
	return u.Host
}

// endpointUserinfo matches the credentials in any scheme://user:pass@host
// fragment. Driver errors can quote the endpoint verbatim, so diagnostics must
// scrub them before printing.
var endpointUserinfo = regexp.MustCompile(
	`([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s@]*@`,
)

// redactEndpointCredentials removes URL userinfo from diagnostic text without
// disturbing the rest of a driver error.
func redactEndpointCredentials(text string) string {
	return endpointUserinfo.ReplaceAllString(text, "$1")
}

// RequireDocker fails the test immediately when the Docker Engine API daemon
// cannot be reached via client.FromEnv (DOCKER_HOST, the socket, and a socket
// proxy are all respected). Integration tests fundamentally require Docker;
// missing infrastructure fails rather than skips. The client is closed via
// t.Cleanup.
func RequireDocker(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.New(client.FromEnv)
	if err != nil {
		t.Fatalf(
			"docker integration test requires a Docker daemon reachable via "+
				"DOCKER_HOST or the local socket (client: %s)",
			redactEndpointCredentials(err.Error()),
		)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		_ = cli.Close()
		t.Fatalf(
			"docker integration test requires a reachable Docker daemon via "+
				"DOCKER_HOST or the local socket; start Docker or point DOCKER_HOST "+
				"at a running daemon (ping: %s)",
			redactEndpointCredentials(err.Error()),
		)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

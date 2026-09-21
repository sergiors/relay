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
	"testing"

	"relay/internal/testutil"
)

// TestIntegrationHealthRealPath verifies the real `relay health` path exits 0
// when both Redis and Docker are reachable, and exits 1 when Redis is not.
func TestIntegrationHealthRealPath(t *testing.T) {
	testutil.RequireRedis(t)
	testutil.RequireDocker(t)

	// The health command loads full config via config.Load, which exits via
	// logger.Fatalf when a required REDIS_* variable is missing, so provide a
	// discard logger and set all three required variables (REDIS_STREAM and
	// REDIS_GROUP need only be non-empty; only REDIS_URI is pinged).
	logger := testutil.DiscardLogger()
	redisURI := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
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

//go:build integration

// This file exercises the `relay dlq` commands against a real Redis server via
// the production OpenDLQStore path.
//
// Excluded from the default suite by the integration build tag. Running it
// (`go test -tags=integration ./...`) REQUIRES Redis at REDIS_TEST_ADDR (default
// localhost:6379, matching compose.dev.yaml); a missing dependency fails rather
// than skips. Start the documented dev dependencies with
// `docker compose -f compose.dev.yaml up -d`.
package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/testutil"
)

// TestIntegrationDLQCommandsAgainstRedis drives the real `relay dlq` command
// path (openRedisDLQStore -> stream.RedisDLQStore) against a real Redis,
// verifying ls/inspect/rm over the wire.
func TestIntegrationDLQCommandsAgainstRedis(t *testing.T) {
	testutil.RequireRedis(t)

	addr := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	source := fmt.Sprintf("itest-cli-dlq-%d", time.Now().UnixNano())
	t.Setenv("REDIS_URI", addr)
	t.Setenv("REDIS_STREAM", source)
	t.Setenv("REDIS_GROUP", "relay")

	opts, _ := config.RedisOptions(addr)
	cli := redis.NewClient(opts)
	dlqStream := "relay:" + source + ":dlq"
	t.Cleanup(func() {
		_ = cli.Del(context.Background(), dlqStream).Err()
		_ = cli.Close()
	})

	id, err := cli.XAdd(context.Background(), &redis.XAddArgs{
		Stream: dlqStream,
		Values: map[string]any{
			"original_stream":  source,
			"original_id":      "1699999999999-0",
			"group":            "relay",
			"consumer":         "worker-1",
			"event":            `{"event_name":"INSERT","id":7}`,
			"reason":           `invocation exhausted`,
			"function":         "alpha",
			"handler":          "events.a.handler",
			"deliveries":       "9",
			"handler_attempts": "5",
			"timestamp":        time.Now().UTC().Format(time.RFC3339),
		},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}

	deps := testDeps(t)
	deps.OpenDLQ = openRedisDLQStore

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls: %v", err)
	}
	if !strings.Contains(out, id) || !strings.Contains(out, "events.a.handler") {
		t.Fatalf("ls missing the seeded entry:\n%s", out)
	}

	out, _, err = runCLIWithDeps(t, deps, "", "dlq", "inspect", id)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	flat := normWS(out)
	for _, want := range []string{"Function: alpha", "Handler attempts: 5", "Handler: events.a.handler"} {
		if !strings.Contains(flat, want) {
			t.Fatalf("inspect missing %q:\n%s", want, out)
		}
	}
	// The event JSON is pretty-printed across lines.
	if !strings.Contains(out, `"event_name": "INSERT"`) {
		t.Fatalf("inspect event not pretty-printed:\n%s", out)
	}

	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", id); err != nil {
		t.Fatalf("dlq rm: %v", err)
	}
	entries, err := cli.XRange(context.Background(), dlqStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange after rm: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("rm must delete exactly the entry, remaining = %+v", entries)
	}
}

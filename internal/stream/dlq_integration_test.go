//go:build integration

// This file exercises the DLQ store against a real Redis server: stream-order
// listing, exact-ID lookup, single-entry XDEL, and malformed-entry rejection.
//
// Excluded from the default suite by the integration build tag. Running it
// (`go test -tags=integration ./...`) REQUIRES Redis at REDIS_TEST_ADDR (default
// localhost:6379, matching compose.dev.yaml); a missing dependency fails rather
// than skips. Start the documented dev dependencies with
// `docker compose -f compose.dev.yaml up -d`.
package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/testutil"
)

// newDLQStoreEnv builds a Redis-backed DLQ store over a uniquely named source
// stream and cleans it up, so tests are isolated from each other and from any
// running dev relay.
func newDLQStoreEnv(t *testing.T) (*RedisDLQStore, *redis.Client, string) {
	t.Helper()
	testutil.RequireRedis(t)
	addr := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	opts, _ := config.RedisOptions(addr)
	source := fmt.Sprintf("itest-dlq-%d", time.Now().UnixNano())
	store := NewRedisDLQStore(opts, source)
	t.Cleanup(func() {
		_ = store.Close()
	})
	// The store owns its own client; build a second one for assertions/cleanup.
	cli := redis.NewClient(opts)
	t.Cleanup(func() {
		_ = cli.Del(context.Background(), store.stream).Err()
		_ = cli.Close()
	})
	return store, cli, source
}

// TestIntegrationDLQStoreListGetDelete pins the read/delete contract against real
// Redis: entries list in stream order, Get finds an exact ID (and only it), and
// Delete removes exactly one entry.
func TestIntegrationDLQStoreListGetDelete(t *testing.T) {
	store, cli, source := newDLQStoreEnv(t)
	ctx := context.Background()

	id1, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: store.stream,
		Values: dlqPayload(source, "orig-1", "relay", "w1", `{"a":1}`, "boom", "alpha", "h.a", 3, 1),
	}).Result()
	if err != nil {
		t.Fatalf("xadd 1: %v", err)
	}
	id2, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: store.stream,
		Values: dlqPayload(source, "orig-1", "relay", "w1", `{"a":1}`, "boom", "beta", "h.b", 4, 2),
	}).Result()
	if err != nil {
		t.Fatalf("xadd 2: %v", err)
	}

	entries, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 || entries[0].ID != id1 || entries[1].ID != id2 {
		t.Fatalf("List = %+v, want [%s %s] in order", entries, id1, id2)
	}
	if entries[0].Function != "alpha" || entries[0].Handler != "h.a" || entries[0].HandlerAttempts != 1 {
		t.Fatalf("entry 1 = %+v", entries[0])
	}

	got, ok, err := store.Get(ctx, id2)
	if err != nil || !ok {
		t.Fatalf("Get(id2) = (%+v, %v, %v)", got, ok, err)
	}
	if got.ID != id2 || got.Function != "beta" || got.HandlerAttempts != 2 {
		t.Fatalf("Get(id2) = %+v", got)
	}
	if _, ok, err := store.Get(ctx, "0-0"); err != nil || ok {
		t.Fatalf("Get(unknown) = (ok=%v, err=%v), want (false, nil)", ok, err)
	}

	deleted, err := store.Delete(ctx, id1)
	if err != nil || !deleted {
		t.Fatalf("Delete(id1) = (%v, %v), want (true, nil)", deleted, err)
	}
	remaining, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(remaining) != 1 || remaining[0].ID != id2 {
		t.Fatalf("after delete remaining = %+v, want only %s", remaining, id2)
	}
	// Deleting an already-gone ID is a no-op, not an error.
	if deleted, err := store.Delete(ctx, id1); err != nil || deleted {
		t.Fatalf("Delete(gone) = (%v, %v), want (false, nil)", deleted, err)
	}
}

// TestIntegrationDLQStoreMalformedEntry pins that a malformed entry fails the
// listing rather than being silently reinterpreted or skipped.
func TestIntegrationDLQStoreMalformedEntry(t *testing.T) {
	store, cli, _ := newDLQStoreEnv(t)
	ctx := context.Background()

	if _, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: store.stream,
		Values: map[string]any{"original_stream": "events"}, // truncated
	}).Result(); err != nil {
		t.Fatalf("xadd malformed: %v", err)
	}
	if _, err := store.List(ctx); err == nil {
		t.Fatal("a malformed DLQ entry must fail the listing")
	}
}

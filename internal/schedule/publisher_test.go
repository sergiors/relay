//go:build integration

// This file exercises the schedule publisher (atomic publish-if-new Lua script)
// against a real Redis server. It is excluded from the default suite by the
// integration build tag and, like the stream integration suite, REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379); a missing dependency fails the
// affected tests rather than skipping them.
package schedule

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
)

// envOr returns the value of the environment variable key, or fallback when it
// is empty or unset. It mirrors the stream integration suite's helper.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// requireRedis fails the test when the test Redis (REDIS_TEST_ADDR, default
// localhost:6379) is not reachable, instead of skipping: the publisher's
// atomicity guarantees are meaningless without it.
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

// ptestEnv provisions a unique stream name for one test and, on completion,
// cleans up the owned keys (its stream plus any dedup keys under this test file's
// "courses" function-name family). The dedup key scan is bounded to that family
// because the keys are keyed by occurrence identity, not by the stream prefix.
type ptestEnv struct {
	client *redis.Client
	stream string
	prefix string
}

func newPTestEnv(t *testing.T, cli *redis.Client) *ptestEnv {
	t.Helper()
	prefix := fmt.Sprintf("psched-%d", time.Now().UnixNano())
	stream := prefix + "-stream"
	t.Cleanup(func() {
		ctx := context.Background()
		_ = cli.Del(ctx, stream).Err()
		cleanupScheduleKeys(ctx, cli, prefix)
	})
	return &ptestEnv{client: cli, stream: stream, prefix: prefix}
}

// cleanupScheduleKeys scans for and deletes dedup keys in this test file's
// "courses" function-name family (the only function this file publishes).
// Dedup keys are keyed by occurrence identity, never by the stream prefix, so
// the scan is bounded to the `relay:schedule:courses:*` family. Deleting a
// matching key is harmless: it can only re-permit publication of an occurrence
// for the exact fixed test instants, which a live Relay would only hold if it
// had published that exact identity.
func cleanupScheduleKeys(ctx context.Context, cli *redis.Client, prefix string) {
	match := "relay:schedule:courses:*"
	var cursor uint64
	for {
		keys, next, err := cli.Scan(ctx, cursor, match, 0).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = cli.Del(ctx, keys...).Err()
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

// newUniqueOccurrence returns an Occurrence whose ID is unique to this test
// invocation (a nano timestamp in the instant), so its dedup key never collides
// with another test's — the dedup key is keyed by occurrence ID, which is
// independent of the stream/prefix used for isolation.
// occCounter guarantees each newUniqueOccurrence gets a distinct second field
// even when tests run back-to-back within the same second.
var occCounter int64

func newUniqueOccurrence() Occurrence {
	sec := time.Now().Second()
	return Occurrence{
		Function:    "courses",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, sec+int(atomic.AddInt64(&occCounter, 1)), 0, time.UTC),
	}
}

// First publish returns true, writes exactly one stream entry, and leaves a dedup
// key with a positive TTL (~7d).
func TestIntegrationPublishIfNew(t *testing.T) {
	cli := requireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, nil, nil)

	ctx := context.Background()
	o := newUniqueOccurrence()
	published, err := p.PublishOccurrence(ctx, o)
	if err != nil {
		t.Fatalf("PublishOccurrence: %v", err)
	}
	if !published {
		t.Fatal("first publish should report published=true")
	}

	if n := e.client.XLen(ctx, e.stream).Val(); n != 1 {
		t.Fatalf("stream length = %d, want 1", n)
	}
	key := dedupKey(o)
	if v, err := e.client.Get(ctx, key).Result(); err != nil {
		t.Fatalf("dedup key missing: %v", err)
	} else if v != o.ID() {
		t.Fatalf("dedup key value = %q, want the occurrence ID", v)
	}
	ttl := e.client.PTTL(ctx, key).Val()
	if ttl <= 0 {
		t.Fatalf("dedup key PTTL = %s, want positive (~7d)", ttl)
	}
	if ttl > 8*24*time.Hour {
		t.Fatalf("dedup key PTTL = %s, want ~7d not longer", ttl)
	}
}

// A duplicate publication returns (false, nil) and still leaves exactly one entry.
func TestIntegrationDuplicateIsNoOp(t *testing.T) {
	cli := requireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, nil, nil)

	ctx := context.Background()
	o := newUniqueOccurrence()
	if _, err := p.PublishOccurrence(ctx, o); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	published, err := p.PublishOccurrence(ctx, o)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if published {
		t.Fatal("duplicate publish should report published=false")
	}
	if n := e.client.XLen(ctx, e.stream).Val(); n != 1 {
		t.Fatalf("stream length after duplicate = %d, want 1", n)
	}
}

// Different occurrences publish independently, each with its own entry + key.
func TestIntegrationDifferentOccurrencesPublishIndependently(t *testing.T) {
	cli := requireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, nil, nil)

	o1 := newUniqueOccurrence()
	o2 := Occurrence{Function: "courses", Handler: "jobs.cleanup.handler", ScheduledAt: o1.ScheduledAt.Add(time.Minute)}
	o3 := Occurrence{Function: "courses", Handler: "jobs.other.handler", ScheduledAt: o1.ScheduledAt}

	ctx := context.Background()
	for _, o := range []Occurrence{o1, o2, o3} {
		published, err := p.PublishOccurrence(ctx, o)
		if err != nil {
			t.Fatalf("publish %q: %v", o.ID(), err)
		}
		if !published {
			t.Fatalf("first publish of %q reported duplicate", o.ID())
		}
	}
	if n := e.client.XLen(ctx, e.stream).Val(); n != 3 {
		t.Fatalf("stream length = %d, want 3", n)
	}
	// Republishing any of them is a no-op.
	dup, _ := p.PublishOccurrence(ctx, o2)
	if dup {
		t.Fatal("republish of an already-published occurrence reported published=true")
	}
	if n := e.client.XLen(ctx, e.stream).Val(); n != 3 {
		t.Fatalf("stream length after duplicate = %d, want 3", n)
	}
}

// Atomicity/failure: when the stream name is a wrong-type (STRING) key, the
// XADD inside the script fails and the dedup key must NOT exist — no
// key-without-entry window (a failed script leaves neither).
func TestIntegrationPublishFailureLeavesNoKey(t *testing.T) {
	cli := requireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, nil, nil)

	ctx := context.Background()
	// Make the stream name a wrong-type key so the script's XADD errors.
	if r := e.client.Set(ctx, e.stream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}

	o := newUniqueOccurrence()
	_, err := p.PublishOccurrence(ctx, o)
	if err == nil {
		t.Fatal("publish against a wrong-type stream should error")
	}
	key := dedupKey(o)
	n, kerr := e.client.Exists(ctx, key).Result()
	if kerr != nil {
		t.Fatalf("exists dedup key: %v", kerr)
	}
	if n != 0 {
		t.Fatalf("dedup key exists (%d) after a failed script; want no key-without-entry window", n)
	}
}

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
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	oteltrace "go.opentelemetry.io/otel/trace"

	"relay/internal/testutil"
)

// ptestEnv provisions a unique stream name for one test and, on completion,
// cleans up the owned keys (its stream plus the dedup keys under its own
// app-name family). The dedup key scan is bounded to that family because
// the keys are keyed by occurrence identity, not by the stream prefix.
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

// cleanupScheduleKeys scans for and deletes the dedup keys whose app-name
// component is this env's unique prefix. Dedup keys are keyed by occurrence
// identity ("relay:schedule:<app>:<handler>:<instant>"), never by the
// stream prefix, so the scan is bounded to `relay:schedule:<prefix>:*` — this
// test's own keys only. The prefix is generated from [0-9-] so it needs no
// percent-encoding (unlike arbitrary user names in the stream package), and no
// separator it contains can collide with another test's.
func cleanupScheduleKeys(ctx context.Context, cli *redis.Client, prefix string) {
	match := "relay:schedule:" + prefix + ":*"
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

// newUniqueOccurrence returns an Occurrence whose identity is unique to this test
// invocation: its app-name component is the env's unixnano-based prefix, so
// the dedup key (`relay:schedule:<prefix>:...`) never collides with another
// test's and cleanup can be scoped to that prefix. The ScheduledAt instant is a
// fixed, second-aligned test instant; occurrences derived within a test differ by
// instant or schedule name (see TestIntegrationDifferentOccurrencesPublishIndependently).
func newUniqueOccurrence(prefix string) Occurrence {
	return Occurrence{
		App:         prefix,
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
}

// First publish returns true, writes exactly one stream entry, and leaves a dedup
// key holding the occurrence ID with a positive TTL (~7d).
func TestIntegrationPublishIfNewWritesEntryAndTTLedDedupKey(t *testing.T) {
	cli := testutil.RequireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, testutil.DiscardLogger(), nil)

	ctx := context.Background()
	o := newUniqueOccurrence(e.prefix)
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
	cli := testutil.RequireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, testutil.DiscardLogger(), nil)

	ctx := context.Background()
	o := newUniqueOccurrence(e.prefix)
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
	cli := testutil.RequireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, testutil.DiscardLogger(), nil)

	o1 := newUniqueOccurrence(e.prefix)
	o2 := Occurrence{App: e.prefix, Schedule: "cleanup", Handler: "jobs.cleanup.handler", ScheduledAt: o1.ScheduledAt.Add(time.Minute)}
	o3 := Occurrence{App: e.prefix, Schedule: "other", Handler: "jobs.other.handler", ScheduledAt: o1.ScheduledAt}

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
	cli := testutil.RequireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, testutil.DiscardLogger(), nil)

	ctx := context.Background()
	// Make the stream name a wrong-type key so the script's XADD errors.
	if r := e.client.Set(ctx, e.stream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}

	o := newUniqueOccurrence(e.prefix)
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

// Bounded-recovery retry: the cron publisher retries the SAME occurrence after a
// transient failure. Because a failed script leaves NEITHER the dedup key nor an
// entry, the retry must succeed and write exactly one entry (and a further
// retry/recovery is a clean duplicate). This is the atomicity invariant the
// bounded publication recovery depends on.
func TestIntegrationPublishFailureThenRetryPublishesOnce(t *testing.T) {
	cli := testutil.RequireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, testutil.DiscardLogger(), nil)
	ctx := context.Background()
	o := newUniqueOccurrence(e.prefix)

	// First attempt fails (wrong-type stream), leaving no dedup key.
	if r := e.client.Set(ctx, e.stream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}
	if _, err := p.PublishOccurrence(ctx, o); err == nil {
		t.Fatal("first attempt should fail against a wrong-type stream")
	}
	// The transient fault clears; the retry of the SAME occurrence succeeds.
	if err := e.client.Del(ctx, e.stream).Err(); err != nil {
		t.Fatalf("clear wrong-type key: %v", err)
	}
	published, err := p.PublishOccurrence(ctx, o)
	if err != nil {
		t.Fatalf("retry publish: %v", err)
	}
	if !published {
		t.Fatal("retry after a failed attempt should publish")
	}
	if n := e.client.XLen(ctx, e.stream).Val(); n != 1 {
		t.Fatalf("stream length after retry = %d, want 1", n)
	}
	if n := e.client.Exists(ctx, dedupKey(o)).Val(); n != 1 {
		t.Fatalf("dedup key after retry = %d, want 1", n)
	}
	// A later recovery of the same occurrence is a clean duplicate no-op.
	dup, err := p.PublishOccurrence(ctx, o)
	if err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if dup {
		t.Fatal("a repeated recovery of an already-published occurrence must be a duplicate")
	}
	if n := e.client.XLen(ctx, e.stream).Val(); n != 1 {
		t.Fatalf("stream length after repeated recovery = %d, want 1", n)
	}
}

// A publish with a trace context writes the W3C fields as flat stream metadata
// beside the untouched event envelope, so a consumer can continue the trace; a
// publish with no trace context writes exactly the event field and nothing else.
func TestIntegrationPublishWritesTraceFieldsBesideEvent(t *testing.T) {
	cli := testutil.RequireRedis(t)
	e := newPTestEnv(t, cli)
	p := NewPublisher(cli, e.stream, testutil.DiscardLogger(), nil)
	ctx := context.Background()

	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    mustTraceID(t, "4bf92f3577b34da6a3ce929d0e0e4736"),
		SpanID:     mustSpanID(t, "00f067aa0ba902b7"),
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})
	traced := oteltrace.ContextWithRemoteSpanContext(ctx, sc)
	tracedID := newUniqueOccurrence(e.prefix)
	if _, err := p.PublishOccurrence(traced, tracedID); err != nil {
		t.Fatalf("publish traced: %v", err)
	}
	entries, err := cli.XRange(ctx, e.stream, "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("XRange = %d entries (err %v), want 1", len(entries), err)
	}
	values := entries[0].Values
	env, _ := tracedID.Envelope()
	if values["event"] != string(env) {
		t.Errorf("event = %v, want the untouched envelope", values["event"])
	}
	if values["traceparent"] == "" || values["traceparent"] == traceparent {
		// The injected traceparent belongs to the schedule.publish child and must
		// carry the remote TRACE id (not equal the remote span's own header).
		t.Errorf("traceparent = %v, want the injected schedule.publish traceparent", values["traceparent"])
	}

	// A second, untraced occurrence writes only the event field.
	plain := newUniqueOccurrence(e.prefix)
	plain.ScheduledAt = plain.ScheduledAt.Add(time.Minute)
	if _, err := p.PublishOccurrence(ctx, plain); err != nil {
		t.Fatalf("publish plain: %v", err)
	}
	entries, err = cli.XRange(ctx, e.stream, "-", "+").Result()
	if err != nil || len(entries) != 2 {
		t.Fatalf("XRange = %d entries (err %v), want 2", len(entries), err)
	}
	plainValues := entries[1].Values
	if len(plainValues) != 1 {
		t.Errorf("untraced entry has fields %v, want only event", plainValues)
	}
	if _, ok := plainValues["traceparent"]; ok {
		t.Error("untraced entry carries a traceparent field")
	}
}

func mustTraceID(t *testing.T, hex string) oteltrace.TraceID {
	t.Helper()
	id, err := oteltrace.TraceIDFromHex(hex)
	if err != nil {
		t.Fatalf("trace id %q: %v", hex, err)
	}
	return id
}

func mustSpanID(t *testing.T, hex string) oteltrace.SpanID {
	t.Helper()
	id, err := oteltrace.SpanIDFromHex(hex)
	if err != nil {
		t.Fatalf("span id %q: %v", hex, err)
	}
	return id
}

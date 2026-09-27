//go:build integration

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// requireAckedTrimMode fails a retention integration test on pre-8.2 Redis,
// whose server rejects the ACKED trim mode. Retention is intentionally disabled
// on such servers, so the ACKED-specific assertions are meaningful only where
// the mode is supported. It probes with a throwaway (nonexistent) key: a
// capability rejection is returned before Redis touches the key, so the probe
// creates no state.
func requireAckedTrimMode(t *testing.T, cli *redis.Client) {
	t.Helper()
	if err := cli.XTrimMinIDApproxMode(context.Background(), "relay:retention-probe-nonexistent", "0-0", 0, trimModeAcked).Err(); err != nil {
		if isUnsupportedTrimMode(err) {
			t.Fatalf("retention integration test requires a Redis server supporting XTRIM ... ACKED (Redis 8.2+); got %v", err)
		}
		t.Fatalf("probe ACKED trim mode: %v", err)
	}
}

// TestIntegrationRetentionTrimsOldEntries verifies the retention trim against a
// real Redis: with no consumer group referencing them, entries older than the
// retention window are removed by retentionTick, while a recent entry remains.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES Redis at REDIS_TEST_ADDR
// (default localhost:6379, matching compose.dev.yaml); a missing dependency fails
// the affected tests rather than skipping them. Start the documented dev
// dependencies with `docker compose -f compose.dev.yaml up -d`.
func TestIntegrationRetentionTrimsOldEntries(t *testing.T) {
	cli := testutil.RequireRedis(t)
	t.Cleanup(func() { _ = cli.Close() })
	requireAckedTrimMode(t, cli)

	stream := fmt.Sprintf("relay:retention-test:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = cli.Del(context.Background(), stream).Err() })

	now := time.Now()
	oldMillis := now.Add(-2 * time.Hour).UnixMilli()
	recentMillis := now.Add(-time.Second).UnixMilli()

	// Stream-node granularity: XTRIM ~ MINID only removes whole internal stream
	// nodes (listpack blocks of up to stream-node-max-entries, default 100), so
	// entries older than the cutoff that share a node with fresh entries are NOT
	// removed by an approximate trim. To test the trim deterministically we fill
	// exactly one full node (100 entries) with old IDs followed by one recent
	// entry: the leading node is entirely past the cutoff, so "~" removes it and
	// leaves the recent entry. (Verified against real Redis 8.10.1: 99 old +
	// 1 recent removes nothing under ~; 100 old + 1 recent removes all 100.)
	const oldCount = 100
	for i := 0; i < oldCount; i++ {
		if _, err := cli.XAdd(context.Background(), &redis.XAddArgs{
			Stream: stream,
			ID:     fmt.Sprintf("%d-%d", oldMillis, i+1),
			Values: map[string]any{"event": "old"},
		}).Result(); err != nil {
			t.Fatalf("xadd old %d: %v", i, err)
		}
	}
	if _, err := cli.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		ID:     fmt.Sprintf("%d-1", recentMillis),
		Values: map[string]any{"event": "recent"},
	}).Result(); err != nil {
		t.Fatalf("xadd recent: %v", err)
	}

	// Trim with a 1h window: the full leading node of old entries (2h old) must
	// be removed, the recent entry (1s old) must remain.
	if err := retentionTick(context.Background(), cli, stream, time.Hour, time.Now, testutil.DiscardLogger()); err != nil {
		t.Fatalf("retentionTick: %v", err)
	}

	msgs, err := cli.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("xrange len = %d, want 1 (old entries trimmed, recent remains): %+v", len(msgs), msgs)
	}
	if msgs[0].ID != fmt.Sprintf("%d-1", recentMillis) {
		t.Fatalf("remaining entry = %q, want recent %d-1", msgs[0].ID, recentMillis)
	}
}

// TestIntegrationRetentionAckedProtectsPending verifies the core safety property
// of ACKED mode against a real Redis: a consumer group that has read but NOT
// acknowledged an entry blocks the trim of that entry (and the ACKED trim stops
// at the oldest referenced entry), while a fully-acknowledged entry is removed.
//
// Setup mirrors the stream-node granularity requirement: one full old node
// (which will be acked) is followed by a second old node referenced by a pending
// entry, then one recent entry. After the first node is acknowledged, an ACKED
// trim removes the acknowledged node but must preserve the pending-referenced
// node.
func TestIntegrationRetentionAckedProtectsPending(t *testing.T) {
	cli := testutil.RequireRedis(t)
	t.Cleanup(func() { _ = cli.Close() })
	requireAckedTrimMode(t, cli)

	ctx := context.Background()
	stream := fmt.Sprintf("relay:retention-acked:%d", time.Now().UnixNano())
	group := stream + "-group"
	t.Cleanup(func() { _ = cli.Del(ctx, stream).Err() })

	now := time.Now()
	// Two full old nodes so each "~" pass can drop a whole node, plus one recent.
	ackedMillis := now.Add(-3 * time.Hour).UnixMilli()
	pendingMillis := now.Add(-2 * time.Hour).UnixMilli()
	recentMillis := now.Add(-time.Second).UnixMilli()

	const nodeSize = 100
	var ackedIDs, pendingIDs []string
	for i := 0; i < nodeSize; i++ {
		id := fmt.Sprintf("%d-%d", ackedMillis, i+1)
		if _, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: id, Values: map[string]any{"event": "acked"}}).Result(); err != nil {
			t.Fatalf("xadd acked %d: %v", i, err)
		}
		ackedIDs = append(ackedIDs, id)
	}
	for i := 0; i < nodeSize; i++ {
		id := fmt.Sprintf("%d-%d", pendingMillis, i+1)
		if _, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: id, Values: map[string]any{"event": "pending"}}).Result(); err != nil {
			t.Fatalf("xadd pending %d: %v", i, err)
		}
		pendingIDs = append(pendingIDs, id)
	}
	recentID := fmt.Sprintf("%d-1", recentMillis)
	if _, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: recentID, Values: map[string]any{"event": "recent"}}).Result(); err != nil {
		t.Fatalf("xadd recent: %v", err)
	}

	if err := cli.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatalf("xgroup create: %v", err)
	}
	// Read the acked node but NOT the pending node into the PEL, then ack it.
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "c", Streams: []string{stream, ">"}, Count: nodeSize,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	if err := cli.XAck(ctx, stream, group, ackedIDs...).Err(); err != nil {
		t.Fatalf("xack acked node: %v", err)
	}

	// ACKED trim at a cutoff past both old nodes: only the fully-acked node may
	// be removed; the pending-referenced node and the recent entry must remain.
	if err := retentionTick(ctx, cli, stream, time.Hour, time.Now, testutil.DiscardLogger()); err != nil {
		t.Fatalf("retentionTick: %v", err)
	}

	msgs, err := cli.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	got := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		got[m.ID] = true
	}
	if got[ackedIDs[0]] {
		t.Fatalf("acknowledged entry %s was not trimmed", ackedIDs[0])
	}
	for _, id := range pendingIDs {
		if !got[id] {
			t.Fatalf("pending-referenced entry %s must be preserved by ACKED trim", id)
		}
	}
	if !got[recentID] {
		t.Fatalf("recent entry %s must be preserved", recentID)
	}
}

// TestIntegrationRetentionAckedMultipleGroups verifies that ACKED protects an
// entry referenced by ANY consumer group: trimming an old entry requires every
// group to have acknowledged it. A second group's outstanding reference keeps
// the entry until that group also acks.
func TestIntegrationRetentionAckedMultipleGroups(t *testing.T) {
	cli := testutil.RequireRedis(t)
	t.Cleanup(func() { _ = cli.Close() })
	requireAckedTrimMode(t, cli)

	ctx := context.Background()
	stream := fmt.Sprintf("relay:retention-mgroups:%d", time.Now().UnixNano())
	groupA := stream + "-a"
	groupB := stream + "-b"
	t.Cleanup(func() { _ = cli.Del(ctx, stream).Err() })

	now := time.Now()
	oldMillis := now.Add(-2 * time.Hour).UnixMilli()
	recentMillis := now.Add(-time.Second).UnixMilli()

	// One full old node (so "~" can drop it) plus a recent entry.
	const nodeSize = 100
	var oldIDs []string
	for i := 0; i < nodeSize; i++ {
		id := fmt.Sprintf("%d-%d", oldMillis, i+1)
		if _, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: id, Values: map[string]any{"event": "old"}}).Result(); err != nil {
			t.Fatalf("xadd old %d: %v", i, err)
		}
		oldIDs = append(oldIDs, id)
	}
	recentID := fmt.Sprintf("%d-1", recentMillis)
	if _, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: recentID, Values: map[string]any{"event": "recent"}}).Result(); err != nil {
		t.Fatalf("xadd recent: %v", err)
	}

	for _, g := range []string{groupA, groupB} {
		if err := cli.XGroupCreateMkStream(ctx, stream, g, "0").Err(); err != nil {
			t.Fatalf("xgroup create %s: %v", g, err)
		}
	}

	// Group A reads and acks the old node; group B has not read it at all, so its
	// implicit reference (the entry is not beyond its last-delivered-id) must keep
	// it alive.
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: groupA, Consumer: "a", Streams: []string{stream, ">"}, Count: nodeSize,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup A: %v", err)
	}
	if err := cli.XAck(ctx, stream, groupA, oldIDs...).Err(); err != nil {
		t.Fatalf("xack A: %v", err)
	}
	if err := retentionTick(ctx, cli, stream, time.Hour, time.Now, testutil.DiscardLogger()); err != nil {
		t.Fatalf("retentionTick (B untouched): %v", err)
	}
	msgs, err := cli.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(msgs) == 0 || msgs[0].ID != oldIDs[0] {
		t.Fatalf("group B's outstanding reference must protect the old node; xrange len=%d first=%v", len(msgs), firstID(msgs))
	}

	// Group B now reads and acks the same node: every group has acknowledged it,
	// so the ACKED trim removes it while the recent entry stays.
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: groupB, Consumer: "b", Streams: []string{stream, ">"}, Count: nodeSize,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup B: %v", err)
	}
	if err := cli.XAck(ctx, stream, groupB, oldIDs...).Err(); err != nil {
		t.Fatalf("xack B: %v", err)
	}
	if err := retentionTick(ctx, cli, stream, time.Hour, time.Now, testutil.DiscardLogger()); err != nil {
		t.Fatalf("retentionTick (both acked): %v", err)
	}
	msgs, err = cli.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	for _, m := range msgs {
		if m.ID == oldIDs[0] {
			t.Fatalf("old node must be trimmed once every group has acked it")
		}
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].ID != recentID {
		t.Fatalf("recent entry must remain; got %v", firstID(msgs))
	}
}

// firstID returns the ID of the first message or "" for an empty slice; a small
// test helper for failure messages.
func firstID(msgs []redis.XMessage) string {
	if len(msgs) == 0 {
		return ""
	}
	return msgs[0].ID
}

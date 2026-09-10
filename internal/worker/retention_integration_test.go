//go:build integration

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestIntegrationRetentionTrimsOldEntries verifies the retention trim against a
// real Redis: entries older than the retention window are removed by
// retentionTick, while a recent entry remains. It uses the same redisAddr()
// helper and build tag as the other integration tests, so `go test ./...`
// skips it by default.
func TestIntegrationRetentionTrimsOldEntries(t *testing.T) {
	if !redisAvailable(t) {
		t.Skip("redis not available")
	}
	cli := redis.NewClient(&redis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = cli.Close() })

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
	retentionTick(context.Background(), cli, stream, time.Hour, time.Now, discardLogger())

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

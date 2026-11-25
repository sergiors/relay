//go:build integration

// Focused F-03 regression coverage for the atomic retention skip against a
// REAL Redis server, using the production trimmer (stream.AckedTrimmer, the
// single Lua script that verifies the Relay group and trims in one step):
//
//   - while the configured Relay group is absent, retention SKIPS the tick
//     atomically — no entries are trimmed, and retention never recreates the
//     group (even when another group on the stream exists);
//   - once the Relay group is recreated while the other group is still
//     present, trimming resumes.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379) with XTRIM ACKED support
// (Redis 8.2+), failing rather than skipping.
package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestIntegrationRetentionSkipsWhileRelayGroupAbsentAndResumes pins the
// group-existence check and the trim being ONE atomic script: with another
// consumer group present but the Relay group missing, a tick refuses entirely
// (entries preserved, group not recreated, retention not disabled), and after
// the Relay group is recreated the same trimmer resumes trimming while the
// other group stays present.
func TestIntegrationRetentionSkipsWhileRelayGroupAbsentAndResumes(t *testing.T) {
	cli := testutil.RequireRedis(t)
	requireAckedTrimMode(t, cli)
	ctx := context.Background()

	streamName := fmt.Sprintf("relay:retention-absent:%d", time.Now().UnixNano())
	relayGroup := streamName + "-relay"
	otherGroup := streamName + "-other"
	t.Cleanup(func() { _ = cli.Del(ctx, streamName).Err() })

	// One full old node (so an approximate "~" pass can drop it) plus a recent
	// entry, mirroring the stream-node granularity the other retention
	// integration tests verify.
	now := time.Now()
	oldMillis := now.Add(-2 * time.Hour).UnixMilli()
	recentMillis := now.Add(-time.Second).UnixMilli()
	const nodeSize = 100
	var oldIDs []string
	for i := 0; i < nodeSize; i++ {
		id := fmt.Sprintf("%d-%d", oldMillis, i+1)
		if _, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, ID: id, Values: map[string]any{"value": "old"},
		}).Result(); err != nil {
			t.Fatalf("xadd old %d: %v", i, err)
		}
		oldIDs = append(oldIDs, id)
	}
	recentID := fmt.Sprintf("%d-1", recentMillis)
	if _, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName, ID: recentID, Values: map[string]any{"value": "recent"},
	}).Result(); err != nil {
		t.Fatalf("xadd recent: %v", err)
	}

	// ANOTHER consumer group exists on the stream, created after the entries
	// at "$" so it never references them (it must not block the later trim).
	// The configured Relay group does NOT exist.
	if err := cli.XGroupCreate(ctx, streamName, otherGroup, "$").Err(); err != nil {
		t.Fatalf("xgroup create other: %v", err)
	}
	trimmer := stream.NewAckedTrimmer(cli, relayGroup)

	// When the Relay group is absent, the tick is skipped atomically: reported
	// as a group-absent skip (never a capability rejection that would disable
	// retention)...
	err := retentionTick(ctx, trimmer, streamName, time.Hour, time.Now, testutil.DiscardLogger())
	if !errors.Is(err, errRetentionGroupAbsent) {
		t.Fatalf("retentionTick with the Relay group absent = %v, want errRetentionGroupAbsent", err)
	}
	if errors.Is(err, errTrimModeUnsupported) {
		t.Fatalf("a missing group must never disable retention: %v", err)
	}
	// ...NO entry is trimmed (the skip and the trim share one script), the
	// Relay group is not recreated, and the other group is untouched.
	msgs, err := cli.XRange(ctx, streamName, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(msgs) != nodeSize+1 {
		t.Fatalf("stream entries = %d, want %d (a group-absent tick must trim nothing)", len(msgs), nodeSize+1)
	}
	groupNames := groupNamesOf(t, cli, streamName)
	if len(groupNames) != 1 || groupNames[0] != otherGroup {
		t.Fatalf("groups = %v, want only %q (retention must never recreate the Relay group)", groupNames, otherGroup)
	}

	// When the Relay group is recreated while the OTHER group is still
	// present, trimming resumes: the Relay group reads and acknowledges the
	// old node (the other group is already past it), the tick succeeds, and
	// the acknowledged old node is trimmed while the recent entry and both
	// groups survive.
	if err := cli.XGroupCreate(ctx, streamName, relayGroup, "0").Err(); err != nil {
		t.Fatalf("xgroup create relay: %v", err)
	}
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: relayGroup, Consumer: "c", Streams: []string{streamName, ">"}, Count: nodeSize,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup relay: %v", err)
	}
	if err := cli.XAck(ctx, streamName, relayGroup, oldIDs...).Err(); err != nil {
		t.Fatalf("xack relay: %v", err)
	}

	if err := retentionTick(ctx, trimmer, streamName, time.Hour, time.Now, testutil.DiscardLogger()); err != nil {
		t.Fatalf("retentionTick after the group returned = %v, want success (trimming resumes)", err)
	}
	remaining, err := cli.XRange(ctx, streamName, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(remaining) != 1 || remaining[0].ID != recentID {
		t.Fatalf("remaining entries = %v, want only the recent %s (the old node must be trimmed)",
			firstID(remaining), recentID)
	}
	names := groupNamesOf(t, cli, streamName)
	if len(names) != 2 {
		t.Fatalf("groups = %v, want both %q and %q to survive the resumed trim", names, otherGroup, relayGroup)
	}
}

// groupNamesOf returns every consumer-group name on the stream (empty on a
// missing stream), for skip/resume assertions that must not disturb group
// state.
func groupNamesOf(t *testing.T, cli *redis.Client, streamName string) []string {
	t.Helper()
	groups, err := cli.XInfoGroups(context.Background(), streamName).Result()
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}
	return names
}

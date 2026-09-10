package worker

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// streamTrimmer is the narrow seam the retention loop needs from Redis: a
// single approximate MINID trim. *redis.Client satisfies it; a test stub can
// implement it with a canned *redis.IntCmd, so unit tests never need a real
// Redis.
type streamTrimmer interface {
	XTrimMinIDApprox(ctx context.Context, key string, minID string, limit int64) *redis.IntCmd
}

// Retention tick cadence. The interval is derived from the retention window
// (retention/24, clamped to [1m, 1h]) rather than being a second env var, so
// there is exactly one knob to reason about and the cadence scales with the
// window. The reasoning:
//
//   - retention/24 gives a coarse-grained cadence: for the canonical 6h window
//     that is 15 minutes, so the stream is trimmed ~4 times per window. A
//     coarse cadence keeps the trim command (a single XTRIM per tick) cheap and
//     avoids hammering Redis.
//   - The 1m floor keeps short retentions (and tests) usable: a 1m window still
//     trims every minute, so an already-large stream is bounded promptly.
//   - The 1h ceiling bounds the worst-case staleness: even a multi-day window
//     trims at least hourly, so entries never linger far past the window.
//
// Bounded staleness is at most one interval: an entry older than the window is
// removed on the first tick after it crosses the cutoff, so it can survive at
// most `interval` past the window.
func retentionTickInterval(retention time.Duration) time.Duration {
	interval := retention / 24
	if interval < time.Minute {
		interval = time.Minute
	}
	if interval > time.Hour {
		interval = time.Hour
	}
	return interval
}

// retentionCutoffID renders a time.Time as a Redis Stream ID in the
// "<unix-milliseconds>-0" form, the MINID threshold for XTRIM. The sequence
// part is always 0 so the cutoff is the earliest possible ID at that
// millisecond: every entry with an ID at or before this millisecond is trimmed.
func retentionCutoffID(now time.Time) string {
	return strconv.FormatInt(now.UnixMilli(), 10) + "-0"
}

// trimStream issues a single approximate MINID trim against the stream. It is a
// thin typed wrapper over the go-redis API (XTRIM <stream> MINID ~ <minID>,
// LIMIT omitted because limit==0) so the retention loop never touches raw
// commands. It returns the number of entries removed (approximate under the
// "~" rule).
//
// Approximate-trim granularity: Redis implements "~" over whole internal stream
// nodes (listpack blocks of up to stream-node-max-entries, default 100), so
// entries older than the cutoff that share a node with fresh entries are left
// in place and removed on a later pass (or when their node fully crosses the
// cutoff). Repeated ticks make progress toward the cutoff one node at a time;
// staleness is bounded by the tick cadence plus one node of entries, not by a
// single-entry guarantee.
func trimStream(ctx context.Context, client streamTrimmer, stream, cutoffID string) (int64, error) {
	return client.XTrimMinIDApprox(ctx, stream, cutoffID, 0).Result()
}

// retentionTick computes the cutoff (now - retention) and performs one trim,
// logging the outcome. It is the unit of work shared by the initial trim and
// every ticker tick. A trim failure is logged and swallowed — retention is
// best-effort and must never stop the worker; the caller retries on the next
// tick. now is injectable for deterministic tests.
func retentionTick(
	ctx context.Context,
	client streamTrimmer,
	stream string,
	retention time.Duration,
	now func() time.Time,
	logger *log.Logger,
) {
	cutoffID := retentionCutoffID(now().Add(-retention))
	n, err := trimStream(ctx, client, stream, cutoffID)
	if err != nil {
		logger.Printf("retention: trim %q: %v (will retry next tick)", stream, err)
		return
	}
	logger.Printf("retention: trimmed %q at cutoff %s (removed %d)", stream, cutoffID, n)
}

// retentionLoop is the periodic stream-retention loop. It runs in its own
// goroutine owned by the worker lifecycle and stops when ctx is cancelled. It
// performs one initial trim shortly after startup (so an already-large stream
// does not wait a full interval) and then trims once per ticker tick.
//
// Each trim is bounded by a short per-trim context so a hung Redis can never
// stack trims or block shutdown; a trim failure is logged and retried on the
// next tick, never fatal. Retention applies to the WHOLE stream, not just
// Relay's consumer group: other consumer groups on the same stream may lose
// unprocessed entries older than the window; fan-out is preserved for entries
// inside the window.
func retentionLoop(
	ctx context.Context,
	client streamTrimmer,
	stream string,
	retention time.Duration,
	logger *log.Logger,
) {
	interval := retentionTickInterval(retention)
	logger.Printf("retention: enabled for stream %q (window %s, tick %s)", stream, retention, interval)

	// Initial trim immediately (bounded) so an already-large stream is trimmed
	// without waiting a full interval. A failure here is logged and the loop
	// continues; the next tick retries.
	initCtx, initCancel := context.WithTimeout(ctx, 30*time.Second)
	retentionTick(initCtx, client, stream, retention, time.Now, logger)
	initCancel()

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			trimCtx, trimCancel := context.WithTimeout(ctx, 30*time.Second)
			retentionTick(trimCtx, client, stream, retention, time.Now, logger)
			trimCancel()
		}
	}
}

package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// trimModeAcked is the XTRIM trim-reference mode Relay requires. Unlike the
// Redis default (KEEPREF), ACKED evicts an entry ONLY when every consumer group
// on the stream has read and acknowledged it. Entries still pending in Relay's
// own group — or in ANY other group on the same stream, including a group that
// has not read them yet — are therefore never trimmed out from under a
// consumer. This is what makes retention safe on a shared stream.
const trimModeAcked = "ACKED"

// errTrimModeUnsupported marks a Redis server that rejects the ACKED mode token
// (pre-8.2 servers, where the mode is parsed but unknown). Retention must be
// DISABLED when it surfaces: falling back to the default KEEPREF (or the older
// mode-less form) would evict entries still pending in a consumer group, which
// is exactly the data loss this mode exists to prevent. It is never swallowed
// and never retried; it terminates the retention loop after a loud log.
var errTrimModeUnsupported = errors.New("redis XTRIM does not support the ACKED trim mode")

// streamTrimmer is the narrow seam the retention loop needs from Redis: a
// single approximate MINID trim carrying an explicit trim-reference mode.
// *redis.Client satisfies it; a test stub can implement it with a canned
// *redis.IntCmd, so unit tests never need a real Redis.
type streamTrimmer interface {
	XTrimMinIDApproxMode(ctx context.Context, key string, minID string, limit int64, mode string) *redis.IntCmd
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

// trimStream issues a single approximate MINID trim against the stream in ACKED
// mode. It is a thin typed wrapper over the go-redis API
// (XTRIM <stream> MINID ~ <minID> ACKED, LIMIT omitted because limit==0) so the
// retention loop never touches raw commands. It returns the number of entries
// removed (approximate under the "~" rule).
//
// Approximate-trim granularity: Redis implements "~" over whole internal stream
// nodes (listpack blocks of up to stream-node-max-entries, default 100), so
// entries older than the cutoff that share a node with fresh entries are left
// in place and removed on a later pass (or when their node fully crosses the
// cutoff). Repeated ticks make progress toward the cutoff one node at a time;
// staleness is bounded by the tick cadence plus one node of entries, not by a
// single-entry guarantee.
func trimStream(ctx context.Context, client streamTrimmer, stream, cutoffID string) (int64, error) {
	return client.XTrimMinIDApproxMode(ctx, stream, cutoffID, 0, trimModeAcked).Result()
}

// isUnsupportedTrimMode reports whether err is Redis rejecting the ACKED mode
// token. A pre-8.2 server replies "ERR syntax error" for the unknown trailing
// mode; because Redis parses the whole command before touching the key, such a
// rejection is returned without trimming anything, so it is safe to treat it as
// a capability signal.
//
// Classification is by error text, not by the concrete redis.Error type: the
// go-redis error type lives in an internal package and cannot be reconstructed
// in tests, and the match is deliberately narrow ("syntax error"/"unknown
// argument"). Both failure directions are safe here — a false positive merely
// disables retention (no trimming), and a false negative retries a failing
// command (still no trimming) — so a transient network/timeout error, which
// never carries those phrases, is never misread as a capability gap in a way
// that could evict data.
func isUnsupportedTrimMode(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "syntax error") || strings.Contains(msg, "unknown argument")
}

// retentionTick computes the cutoff (now - retention) and performs one ACKED
// trim, logging the outcome. It is the unit of work shared by the initial trim
// and every ticker tick. now is injectable for deterministic tests.
//
// It returns errTrimModeUnsupported when the server rejects the ACKED mode; the
// caller disables retention rather than trimming unsafely. Any other failure is
// logged and swallowed — retention is best-effort and must never stop the
// worker; the caller retries on the next tick.
func retentionTick(
	ctx context.Context,
	client streamTrimmer,
	stream string,
	retention time.Duration,
	now func() time.Time,
	logger *slog.Logger,
) error {
	cutoffID := retentionCutoffID(now().Add(-retention))
	n, err := trimStream(ctx, client, stream, cutoffID)
	if err != nil {
		if isUnsupportedTrimMode(err) {
			return fmt.Errorf("%w: %v", errTrimModeUnsupported, err)
		}
		logger.Warn("Retention: trim failed; will retry next tick", "stream", stream, "error", err)
		return nil
	}
	logger.Debug("Retention: trimmed stream", "stream", stream, "cutoff", cutoffID, "mode", trimModeAcked, "removed", n)
	return nil
}

// logRetentionUnsupported reports the permanent disable of retention with the
// operator action needed. It is logged once, at Error, because a configured
// retention window silently doing nothing is a correctness surprise: the
// operator asked for trimming and must know it is refused (and why) rather than
// discovering it from an unbounded stream — or, worse, getting an unsafe trim.
func logRetentionUnsupported(logger *slog.Logger, stream string, err error) {
	logger.Error(
		"Retention: disabled; Redis rejected the ACKED trim mode (requires Redis 8.2+); "+
			"refusing to trim with the unsafe default, which can evict entries still pending in a consumer group. "+
			"Upgrade Redis or unset REDIS_STREAM_RETENTION",
		"stream", stream,
		"error", err,
	)
}

// retentionLoop is the periodic stream-retention loop. It runs in its own
// goroutine owned by the worker lifecycle and stops when ctx is cancelled or
// when the server refuses the ACKED mode. It performs one initial trim shortly
// after startup (so an already-large stream does not wait a full interval) and
// then trims once per ticker tick.
//
// Each trim is bounded by a short per-trim context so a hung Redis can never
// stack trims or block shutdown; a transient trim failure is logged and retried
// on the next tick, never fatal. Retention uses XTRIM ... ACKED, so it applies
// to the WHOLE stream but only removes entries acknowledged by EVERY consumer
// group: entries pending in Relay's group or in any other group (including a
// group that has not read them yet) are preserved. A group that never reads or
// acks its entries therefore blocks trimming of the covered range — that is the
// safe direction (no data loss), and the operator must retire or drain such a
// group.
//
// If the server does not support ACKED (pre-8.2), the loop logs and returns
// WITHOUT trimming: it never falls back to the unsafe KEEPREF/default behavior.
func retentionLoop(
	ctx context.Context,
	client streamTrimmer,
	stream string,
	retention time.Duration,
	logger *slog.Logger,
) {
	interval := retentionTickInterval(retention)
	logger.Info("Retention: starting", "stream", stream, "window", retention, "tick", interval, "mode", trimModeAcked)

	// Initial trim immediately (bounded) so an already-large stream is trimmed
	// without waiting a full interval. It doubles as the capability check: a
	// server that rejects ACKED returns before touching any entry, so retention
	// is disabled with a clear log instead of trimming unsafely. A transient
	// failure is logged by retentionTick and the loop continues.
	initCtx, initCancel := context.WithTimeout(ctx, 30*time.Second)
	initErr := retentionTick(initCtx, client, stream, retention, time.Now, logger)
	initCancel()
	if errors.Is(initErr, errTrimModeUnsupported) {
		logRetentionUnsupported(logger, stream, initErr)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			trimCtx, trimCancel := context.WithTimeout(ctx, 30*time.Second)
			trimErr := retentionTick(trimCtx, client, stream, retention, time.Now, logger)
			trimCancel()
			if errors.Is(trimErr, errTrimModeUnsupported) {
				// The server changed under us (downgrade/rollback): stop
				// trimming rather than fall back to an unsafe mode.
				logRetentionUnsupported(logger, stream, trimErr)
				return
			}
		}
	}
}

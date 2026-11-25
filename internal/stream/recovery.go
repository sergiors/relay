package stream

import (
	"context"
	"strings"
	"time"

	"relay/internal/observability/metrics"
)

// isNoGroup reports whether err is Redis' NOGROUP consumer-group-missing
// failure. Redis uses the same prefix for a missing stream and a missing group
// ("NOGROUP No such key ... or consumer group ..."), and both are repaired the
// same way: ensureGroup creates the stream (MKSTREAM) and the group at ID 0.
//
// Classification is deliberately textual: the concrete go-redis error type for
// a server reply lives in an internal package and cannot be reconstructed in
// tests, and Lua scripts surface a server error reply the same way.
func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

// groupGeneration returns the local consumer-group generation. It advances each
// time this process confirms a group recovery after NOGROUP. A delivery
// captures the generation it started under, so a completion that crosses a
// recreation cannot acknowledge a source entry that the recreated group may not
// have delivered yet (it would be unread, not settled).
func (c *Consumer) groupGeneration() uint64 {
	return c.groupGen.Load()
}

// recoverConsumerGroup recreates the consumer group after a NOGROUP error that
// was observed while the local generation was observedGen.
//
// Coalescing: if another goroutine already recovered the group after the failed
// command began, the generation no longer matches and this call does nothing;
// one XGROUP CREATE repairs the group for every concurrent observer. The whole
// attempt is serialized on recoveryMu, so concurrent NOGROUP observations never
// issue redundant commands. There is deliberately no distributed lock: each
// process repairs its own view, and XGROUP CREATE at ID 0 is idempotent under
// BUSYGROUP.
//
// It returns a bounded, jittered wait before the caller should retry. A single
// successful recovery returns no wait (the caller retries immediately), while
// repeated recovery without an intervening normal consumption success is paced
// so a group that is destroyed as fast as Relay recreates it cannot spin.
//
// Health stays unhealthy across the recovery (see Consumer.Healthy and
// noteConsumeSuccess): only a later successful XREADGROUP issued after this
// attempt clears the recovering state, so an unrelated successful Redis command
// cannot mask a still-missing group.
func (c *Consumer) recoverConsumerGroup(
	ctx context.Context,
	observedGen uint64,
) (time.Duration, error) {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()

	if c.groupGen.Load() != observedGen {
		// The group was already recovered after the failed command was issued.
		return 0, nil
	}

	c.recovering.Store(true)
	// Invalidate every read already in flight: a success from a command issued
	// before this observation cannot confirm the recovery.
	c.recoveryEpoch.Add(1)

	outcome, err := c.recoverGroup(ctx)
	c.recoveryStreak++
	if err != nil {
		c.metrics.IncLabels(metrics.MetricGroupRecoveries,
			[]metrics.Label{
				{Name: "result", Value: metrics.GroupRecoveryFailed},
			})
		return c.recoveryBackoff.next(), err
	}

	c.groupGen.Add(1)
	result := metrics.GroupRecoveryExisting
	if outcome == GroupCreated {
		result = metrics.GroupRecoveryCreated
	}
	c.metrics.IncLabels(metrics.MetricGroupRecoveries,
		[]metrics.Label{{Name: "result", Value: result}})
	c.log.Warn("Redis: consumer group was missing; recovery completed",
		"stream", c.stream,
		"group", c.group,
		"created", outcome == GroupCreated,
		"generation", c.groupGen.Load(),
	)

	// The first recovery retries immediately. If the group disappears again
	// without any successful normal consumption in between (streak > 1), pace
	// further recoveries with the bounded backoff so an external actor deleting
	// the group cannot drive a hot loop of recovery commands.
	if c.recoveryStreak > 1 {
		return c.recoveryBackoff.next(), nil
	}
	return 0, nil
}

// handleGroupLoss classifies err and, when it is NOGROUP, recreates the group
// and reports how long the caller should wait before retrying. It centralizes
// the behavior shared by XREADGROUP and the reclaim XPENDING/XAUTOCLAIM paths:
// only confirmed NOGROUP triggers recovery; every other failure keeps the
// existing health/backoff behavior in the caller.
func (c *Consumer) handleGroupLoss(
	ctx context.Context,
	operation string,
	observedGen uint64,
	err error,
) (handled bool, wait time.Duration) {
	if !isNoGroup(err) {
		return false, 0
	}
	// A NOGROUP is not a transport read failure: it is accounted by the
	// dedicated group-recovery metric instead of redis_read_errors_total.
	wait, recoverErr := c.recoverConsumerGroup(ctx, observedGen)
	c.noteOutcome(err, wait)
	if recoverErr != nil {
		c.log.Warn("Redis: consumer group recovery failed; will retry",
			"stream", c.stream,
			"group", c.group,
			"operation", operation,
			"error", recoverErr,
			"delay", wait,
		)
	}
	return true, wait
}

// waitContext sleeps for delay or until ctx is done. It returns false when ctx
// was cancelled first. A non-positive delay only yields to a live context.
func waitContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestRetentionTickGroupAbsentIsSkipNotDisable pins the classification of the
// production trimmer's "Relay group absent" answer: it is a per-tick SKIP
// (errRetentionGroupAbsent, distinct from the capability rejection), never the
// errTrimModeUnsupported signal that disables retention, and the trim call
// itself still happened exactly once (the check and the trim are one atomic
// script — the script refused, no separate check ran).
func TestRetentionTickGroupAbsentIsSkipNotDisable(t *testing.T) {
	stub := &stubTrimmer{err: stream.ErrRetentionGroupAbsent}
	now := func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }

	err := retentionTick(context.Background(), stub, "relay:events", time.Hour, now, testutil.DiscardLogger())
	if !errors.Is(err, errRetentionGroupAbsent) {
		t.Fatalf("retentionTick error = %v, want errRetentionGroupAbsent", err)
	}
	if errors.Is(err, errTrimModeUnsupported) {
		t.Fatalf("a missing group must never read as an unsupported mode (which would disable retention): %v", err)
	}
	if got := stub.calls(); got != 1 {
		t.Fatalf("trim calls = %d, want 1 (one atomic check-and-trim attempt)", got)
	}
}

// TestRetentionLoopKeepsTickingWhileGroupAbsent pins the loop-level contract:
// an absent Relay group during the initial probe must NOT terminate the
// retention loop (only an unsupported trim mode does) — the loop stays alive
// so trimming resumes on a later tick once the group is back.
func TestRetentionLoopKeepsTickingWhileGroupAbsent(t *testing.T) {
	stub := &stubTrimmer{err: stream.ErrRetentionGroupAbsent}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		retentionLoop(ctx, stub, "relay:events", time.Hour, testutil.DiscardLogger())
	}()

	// The initial probe runs immediately and observes the absent group; the
	// loop must still be running afterwards (a disable would have returned).
	deadline := time.After(2 * time.Second)
	for stub.calls() == 0 {
		select {
		case <-deadline:
			t.Fatal("retentionLoop did not run its initial trim probe")
		case <-time.After(time.Millisecond):
		}
	}
	waitSustainedWorker(t, "retention loop alive while the group is absent", 200*time.Millisecond, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	})

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retentionLoop did not stop on cancel")
	}
}

// waitSustainedWorker polls pred until it holds continuously for dur or the
// 10s budget elapses (failing the test then). The worker package has no
// equivalent of the stream package's waitSustained helper.
func waitSustainedWorker(t *testing.T, what string, dur time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	holdStart := time.Time{}
	for time.Now().Before(deadline) {
		if pred() {
			if holdStart.IsZero() {
				holdStart = time.Now()
			} else if time.Since(holdStart) >= dur {
				return
			}
		} else {
			holdStart = time.Time{}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to hold for %v", what, dur)
}

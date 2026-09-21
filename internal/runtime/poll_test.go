package runtime

import (
	"context"
	"time"
)

// pollInterval is the single retry cadence shared by every bounded poll in the
// test suite. Polls are for bridging asynchronous daemon/goroutine latency (a
// container starting, a label appearing, output being forwarded); they are never
// a fixed sleep standing in for synchronization.
const pollInterval = 25 * time.Millisecond

// pollUntil polls cond every pollInterval until it reports true or timeout
// elapses, returning cond's final result. It is the one bounded-poll helper:
// callers express the readiness condition (reading daemon state or a sink) and
// the deadline, rather than hand-rolling time.Now()/time.Sleep loops. A
// non-nil ctx that is done ends the poll early (returning false) so a test's
// overall timeout is respected. cond runs on the calling goroutine only.
func pollUntil(ctx context.Context, timeout time.Duration, cond func() bool) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		if ctx == nil {
			time.Sleep(pollInterval)
			continue
		}
		t := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// stubTrimmer is a test double for streamTrimmer. It records every
// XTrimMinIDApproxMode call (stream key, cutoff ID, and mode) and returns a
// canned result (value or error) so unit tests never need a real Redis.
type stubTrimmer struct {
	mu       sync.Mutex
	streams  []string
	cutoffID []string
	modes    []string
	val      int64
	err      error
}

func (s *stubTrimmer) XTrimMinIDApproxMode(ctx context.Context, key string, minID string, limit int64, mode string) *redis.IntCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = append(s.streams, key)
	s.cutoffID = append(s.cutoffID, minID)
	s.modes = append(s.modes, mode)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(s.val)
	if s.err != nil {
		cmd.SetErr(s.err)
	}
	return cmd
}

// stubDLQTrimmer is a test double for dlqTrimmer. It records every
// XTrimMinIDApprox call (stream key and cutoff ID, with no mode) and returns a
// canned result so DLQ-retention tests never need a real Redis.
type stubDLQTrimmer struct {
	mu       sync.Mutex
	streams  []string
	cutoffID []string
	val      int64
	err      error
}

func (s *stubDLQTrimmer) XTrimMinIDApprox(ctx context.Context, key string, minID string, limit int64) *redis.IntCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = append(s.streams, key)
	s.cutoffID = append(s.cutoffID, minID)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(s.val)
	if s.err != nil {
		cmd.SetErr(s.err)
	}
	return cmd
}

func (s *stubDLQTrimmer) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *stubDLQTrimmer) lastStream() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.streams) == 0 {
		return ""
	}
	return s.streams[len(s.streams)-1]
}

func (s *stubDLQTrimmer) lastCutoffID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cutoffID) == 0 {
		return ""
	}
	return s.cutoffID[len(s.cutoffID)-1]
}

func (s *stubTrimmer) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *stubTrimmer) lastStream() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.streams) == 0 {
		return ""
	}
	return s.streams[len(s.streams)-1]
}

func (s *stubTrimmer) lastCutoffID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cutoffID) == 0 {
		return ""
	}
	return s.cutoffID[len(s.cutoffID)-1]
}

func (s *stubTrimmer) lastMode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.modes) == 0 {
		return ""
	}
	return s.modes[len(s.modes)-1]
}

// TestRetentionTickInterval pins the pure interval derivation: 6h → 15m, small
// values clamp to the 1m floor, huge values clamp to the 1h ceiling.
func TestRetentionTickInterval(t *testing.T) {
	tests := []struct {
		name      string
		retention time.Duration
		want      time.Duration
	}{
		{"6h -> 15m", 6 * time.Hour, 15 * time.Minute},
		{"1h -> 2m30s", time.Hour, 2*time.Minute + 30*time.Second},
		{"small clamps to 1m floor", time.Minute, time.Minute},
		{"tiny clamps to 1m floor", time.Second, time.Minute},
		{"huge clamps to 1h ceiling", 48 * time.Hour, time.Hour},
		{"very huge clamps to 1h ceiling", 30 * 24 * time.Hour, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retentionTickInterval(tt.retention); got != tt.want {
				t.Fatalf("retentionTickInterval(%v) = %v, want %v", tt.retention, got, tt.want)
			}
		})
	}
}

// TestRetentionCutoffID pins the Stream ID rendering: a fixed time becomes the
// exact "<unix-milliseconds>-0" string.
func TestRetentionCutoffID(t *testing.T) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	want := "1789041600000-0"
	if got := retentionCutoffID(now); got != want {
		t.Fatalf("retentionCutoffID(%v) = %q, want %q", now, got, want)
	}
}

// TestRetentionTickTrimsConfiguredStream verifies retentionTick issues an ACKED
// trim against the configured stream with the correct approximate-MINID cutoff —
// (now - retention) rendered as the "<unix-milliseconds>-0" form — using an
// injected now for determinism. The mode is pinned to ACKED so a regression to
// the unsafe default (KEEPREF) or a mode-less command fails loudly.
func TestRetentionTickTrimsConfiguredStream(t *testing.T) {
	stub := &stubTrimmer{val: 3}
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	retention := 6 * time.Hour // cutoff = now - 6h
	if err := retentionTick(context.Background(), stub, "relay:events", retention, func() time.Time { return now }, testutil.DiscardLogger()); err != nil {
		t.Fatalf("retentionTick returned error: %v", err)
	}

	if got := stub.calls(); got != 1 {
		t.Fatalf("trim calls = %d, want 1", got)
	}
	if got := stub.lastStream(); got != "relay:events" {
		t.Fatalf("trimmed stream = %q, want %q", got, "relay:events")
	}
	if got := stub.lastMode(); got != trimModeAcked {
		t.Fatalf("trim mode = %q, want %q", got, trimModeAcked)
	}
	// now(2026-09-10 12:00 UTC) - 6h = 06:00 UTC = 1789019... millis.
	wantCutoff := retentionCutoffID(now.Add(-retention))
	if got := stub.lastCutoffID(); got != wantCutoff {
		t.Fatalf("cutoff ID = %q, want %q", got, wantCutoff)
	}
	// Pin the exact "<unix-milliseconds>-0" rendering too (catches a future
	// regression that forgets to subtract the retention window — the cutoff
	// must be 6h earlier than now, not now itself).
	wantMillis := now.Add(-retention).UnixMilli()
	if want := fmt.Sprintf("%d-0", wantMillis); wantCutoff != want {
		t.Fatalf("cutoff ID %q is not the expected %q", wantCutoff, want)
	}
}

// TestRetentionTickTransientErrorLoggedAndRetried verifies a transient Redis
// trim failure is logged and swallowed (nil return, never stops the worker), so
// a subsequent tick retries the trim.
func TestRetentionTickTransientErrorLoggedAndRetried(t *testing.T) {
	stub := &stubTrimmer{err: errors.New("redis down")}
	now := func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }

	// Two ticks, both failing: the loop must survive and retry.
	for i := 0; i < 2; i++ {
		if err := retentionTick(context.Background(), stub, "relay:events", time.Hour, now, testutil.DiscardLogger()); err != nil {
			t.Fatalf("transient trim error must be swallowed, got %v", err)
		}
	}

	if got := stub.calls(); got != 2 {
		t.Fatalf("trim calls = %d, want 2 (retried on next tick)", got)
	}
}

// TestRetentionTickUnsupportedModeIsFatalSignal pins that a Redis server
// rejecting the ACKED mode token (pre-8.2: "ERR syntax error") is reported as
// the errTrimModeUnsupported sentinel rather than swallowed. The caller uses it
// to disable retention instead of falling back to an unsafe trim.
func TestRetentionTickUnsupportedModeIsFatalSignal(t *testing.T) {
	stub := &stubTrimmer{err: errors.New("ERR syntax error")}
	now := func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }

	err := retentionTick(context.Background(), stub, "relay:events", time.Hour, now, testutil.DiscardLogger())
	if !errors.Is(err, errTrimModeUnsupported) {
		t.Fatalf("retentionTick error = %v, want errTrimModeUnsupported", err)
	}
}

// TestIsUnsupportedTrimMode distinguishes a capability rejection (an unknown
// trailing mode token) from transient failures: only Redis command errors
// mentioning a syntax/unknown-argument problem qualify, so a network timeout is
// never misread as "Redis does not support ACKED" (which would disable
// retention on a transient blip).
func TestIsUnsupportedTrimMode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"syntax error", errors.New("ERR syntax error"), true},
		{"unknown argument", errors.New("ERR unknown argument 'ACKED'"), true},
		{"redis nil is not a mode rejection", redis.Nil, false},
		{"plain network error", errors.New("dial tcp: connection refused"), false},
		{"timeout", context.DeadlineExceeded, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnsupportedTrimMode(tt.err); got != tt.want {
				t.Fatalf("isUnsupportedTrimMode(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestRetentionLoopStopsOnCancel verifies the loop exits promptly when ctx is
// cancelled. The initial trim runs first (bounded), then the select observes
// ctx.Done before the first tick, so cancellation returns fast.
func TestRetentionLoopStopsOnCancel(t *testing.T) {
	stub := &stubTrimmer{val: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		retentionLoop(ctx, stub, "relay:events", time.Hour, testutil.DiscardLogger())
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retentionLoop did not stop on cancel")
	}
}

// TestRetentionLoopDisablesOnUnsupportedMode pins the safety contract: when the
// initial trim reports the ACKED mode unsupported, retentionLoop must RETURN
// immediately after the single probe trim — it must not keep ticking (and never
// issue a fallback trim). The loop's only Redis round trip is the capability
// probe, which fails without trimming anything.
func TestRetentionLoopDisablesOnUnsupportedMode(t *testing.T) {
	stub := &stubTrimmer{err: errors.New("ERR syntax error")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		retentionLoop(ctx, stub, "relay:events", time.Hour, testutil.DiscardLogger())
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retentionLoop did not disable itself on an unsupported ACKED mode")
	}
	if got := stub.calls(); got != 1 {
		t.Fatalf("trim calls = %d, want exactly 1 (the capability probe, then disable)", got)
	}
	if got := stub.lastMode(); got != trimModeAcked {
		t.Fatalf("probe mode = %q, want %q (never a fallback mode)", got, trimModeAcked)
	}
}

// TestDLQRetentionTickTrimsConfiguredStream verifies dlqRetentionTick issues a
// mode-less approximate MINID trim against the DLQ stream with the correct
// (now - retention) cutoff, using an injected now. The mode-less seam pins that
// the DLQ trim does NOT require ACKED: a pre-8.2 server that disables main
// retention still age-trims the DLQ.
func TestDLQRetentionTickTrimsConfiguredStream(t *testing.T) {
	stub := &stubDLQTrimmer{val: 4}
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	retention := 7 * 24 * time.Hour
	dlq := "relay:events:dlq"
	dlqRetentionTick(context.Background(), stub, dlq, retention, func() time.Time { return now }, testutil.DiscardLogger())

	if got := stub.calls(); got != 1 {
		t.Fatalf("trim calls = %d, want 1", got)
	}
	if got := stub.lastStream(); got != dlq {
		t.Fatalf("trimmed stream = %q, want %q", got, dlq)
	}
	wantCutoff := retentionCutoffID(now.Add(-retention))
	if got := stub.lastCutoffID(); got != wantCutoff {
		t.Fatalf("cutoff ID = %q, want %q", got, wantCutoff)
	}
}

// TestDLQRetentionTickTransientErrorLoggedAndRetried verifies a transient Redis
// trim failure is logged and swallowed (the tick never stops the worker), so a
// subsequent tick retries the trim. Unlike the main stream there is no
// errTrimModeUnsupported signal: an "ERR syntax error" is a transient/unknown
// failure here too.
func TestDLQRetentionTickTransientErrorLoggedAndRetried(t *testing.T) {
	stub := &stubDLQTrimmer{err: errors.New("redis down")}
	now := func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }

	for i := 0; i < 2; i++ {
		dlqRetentionTick(context.Background(), stub, "relay:events:dlq", 7*24*time.Hour, now, testutil.DiscardLogger())
	}

	if got := stub.calls(); got != 2 {
		t.Fatalf("trim calls = %d, want 2 (retried on next tick)", got)
	}
}

// TestDLQRetentionLoopStopsOnCancel verifies the DLQ loop exits promptly when
// ctx is cancelled, mirroring retentionLoop.
func TestDLQRetentionLoopStopsOnCancel(t *testing.T) {
	stub := &stubDLQTrimmer{val: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		dlqRetentionLoop(ctx, stub, "relay:events:dlq", 7*24*time.Hour, testutil.DiscardLogger())
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dlqRetentionLoop did not stop on cancel")
	}
}

// TestDLQRetentionLoopDoesNotRequireAckedMode pins the isolation contract: the
// DLQ loop performs its initial trim and keeps ticking even when Redis would
// reject ACKED for the main stream. Its seam has no mode at all, so it cannot
// fall into the main loop's disable path. We assert it makes at least the
// initial trim (and does not exit as if unsupported).
func TestDLQRetentionLoopDoesNotRequireAckedMode(t *testing.T) {
	stub := &stubDLQTrimmer{val: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		dlqRetentionLoop(ctx, stub, "relay:events:dlq", 30*24*time.Hour, testutil.DiscardLogger())
	}()
	// Wait for the initial trim, then cancel.
	deadline := time.After(2 * time.Second)
	for stub.calls() == 0 {
		select {
		case <-deadline:
			t.Fatal("dlqRetentionLoop did not perform an initial trim")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dlqRetentionLoop did not stop on cancel")
	}
	if got := stub.lastStream(); got != "relay:events:dlq" {
		t.Fatalf("trimmed stream = %q, want relay:events:dlq", got)
	}
}

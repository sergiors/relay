package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"relay/internal/testutil"
)

// TestShutdownSequenceRunsInOrderAndNeverStopsOnError pins the single teardown
// contract the refactor introduced: every step runs in the documented order, and
// a step returning an error is logged without aborting the rest — the same
// non-fatal policy the original inline shutdown tail had. It uses pure seams, so
// it needs no Docker, Redis, or state DB.
func TestShutdownSequenceRunsInOrderAndNeverStopsOnError(t *testing.T) {
	var order []string
	record := func(name string, err error) func() error {
		return func() error {
			order = append(order, name)
			return err
		}
	}

	seq := shutdownSequence{
		stopLifecycle:    func() { order = append(order, "lifecycle") },
		closeSocket:      record("socket", nil),
		stopScheduler:    record("scheduler", errors.New("scheduler boom")),
		joinHousekeeping: record("housekeeping", nil),
		joinServices:     record("join", errors.New("join boom")),
		cleanupServices:  func() { order = append(order, "cleanup") },
		flushStats:       func() { order = append(order, "flush") },
		stopMetrics:      record("metrics", nil),
		stopWebhook:      record("webhook", nil),
		closeManager:     record("manager", errors.New("manager boom")),
		closeState:       record("state", nil),
		closeClient:      record("client", nil),
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	seq.run(logger)

	want := []string{
		"lifecycle", "socket", "scheduler", "housekeeping", "join",
		"cleanup", "flush", "metrics", "webhook", "manager", "state", "client",
	}
	if len(order) != len(want) {
		t.Fatalf("ran %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("step %d = %q, want %q (full order %v)", i, order[i], want[i], order)
		}
	}

	// Each step failure was surfaced, not swallowed, and the teardown still
	// reached its final marker.
	for _, wantMsg := range []string{
		"Scheduler: graceful shutdown failed",
		"Service: coordinator shutdown failed",
		"Runtime: manager close failed",
		"Shutdown complete",
	} {
		if !strings.Contains(logs.String(), wantMsg) {
			t.Errorf("log missing %q:\n%s", wantMsg, logs.String())
		}
	}
}

// TestShutdownSequenceNilSafe verifies a sequence with only the lifecycle cancel
// set (the state right after the Redis client exists, before any resource was
// constructed) is safe: no panic, no skipped final marker.
func TestShutdownSequenceNilSafe(t *testing.T) {
	stopCalled := false
	seq := shutdownSequence{stopLifecycle: func() { stopCalled = true }}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	seq.run(logger)

	if !stopCalled {
		t.Fatal("lifecycle cancel must run even when every other handle is nil")
	}
}

// TestRunInvalidRedisDSNReturnsError proves the worker's pre-resource failure is
// RETURNED, not os.Exit'd: config.Load succeeds once the required env vars are
// set, the DSN parse fails, and Run returns an error naming it. Run returns
// before any resource (Redis client, manager, servers) is created, so the test
// needs no Docker or Redis.
func TestRunInvalidRedisDSNReturnsError(t *testing.T) {
	t.Setenv("REDIS_URI", "redis://%zz")
	t.Setenv("REDIS_STREAM", "test-stream")
	t.Setenv("REDIS_GROUP", "test-group")

	err := Run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("Run returned nil for an invalid REDIS_URI; want an error")
	}
	if !strings.Contains(err.Error(), "redis config invalid") {
		t.Fatalf("error = %q, want it to name the invalid redis config", err)
	}
}

// fakeEnsurer is the consumerGroupEnsurer seam: it returns a canned EnsureGroup
// error so the cancellation-vs-genuine classification is exercised without a
// Redis server.
type fakeEnsurer struct {
	err   error
	calls int
}

func (f *fakeEnsurer) EnsureGroup(context.Context) error {
	f.calls++
	return f.err
}

// TestStartupInterruptedClassification pins the predicate that separates a
// lifecycle cancellation from a genuine startup failure: it is true only when
// the lifecycle is actually cancelled AND the operation error wraps a context
// error. A genuine error merely racing a cancelled lifecycle stays a failure, so
// a real problem is never silently swallowed as graceful shutdown.
func TestStartupInterruptedClassification(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"nil error", cancelled, nil, false},
		{"cancelled ctx, wrapped canceled", cancelled, fmt.Errorf("read: %w", context.Canceled), true},
		{"cancelled ctx, wrapped deadline", cancelled, fmt.Errorf("read: %w", context.DeadlineExceeded), true},
		{"live ctx, wrapped canceled", live, fmt.Errorf("read: %w", context.Canceled), false},
		{"cancelled ctx, genuine error", cancelled, errors.New("connection refused"), false},
		{"live ctx, genuine error", live, errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		if got := startupInterrupted(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: startupInterrupted = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestEnsureGroupCancellationIsGraceful proves the core lifecycle contract: an
// EnsureGroup failure caused by the lifecycle being cancelled during startup is
// classified as errStartupInterrupted, which startupResult converts to a nil
// return, so Run converges through its deferred cleanup and reports success
// rather than a startup error.
func TestEnsureGroupCancellationIsGraceful(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeEnsurer{err: fmt.Errorf("xgroup create: %w", context.Canceled)}
	err := ensureGroup(ctx, c, logger)
	if !errors.Is(err, errStartupInterrupted) {
		t.Fatalf("ensureGroup cancellation error = %v, want errStartupInterrupted", err)
	}
	if got := startupResult(err); got != nil {
		t.Fatalf("startupResult(cancellation) = %v, want nil (graceful shutdown)", got)
	}
	if !strings.Contains(logs.String(), "consumer group creation interrupted by shutdown") {
		t.Errorf("expected an Info log recording the interrupted startup, got:\n%s", logs.String())
	}
}

// TestEnsureGroupGenuineFailureIsReturned proves a real prerequisite failure is
// NOT masked by a coincident cancellation: an error that does not wrap a context
// error is wrapped and returned, and startupResult leaves it non-nil for the CLI
// boundary to print and exit on.
func TestEnsureGroupGenuineFailureIsReturned(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Even with a cancelled lifecycle, a genuine (non-context) error is a real
	// failure, not a graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeEnsurer{err: errors.New("dial tcp 127.0.0.1:6379: connection refused")}
	err := ensureGroup(ctx, c, logger)
	if err == nil {
		t.Fatal("ensureGroup genuine failure = nil, want an error")
	}
	if errors.Is(err, errStartupInterrupted) {
		t.Fatalf("genuine failure was classified as a shutdown: %v", err)
	}
	if !strings.Contains(err.Error(), "ensure consumer group failed") {
		t.Fatalf("error = %q, want it to name the consumer-group failure", err)
	}
	if got := startupResult(err); !errors.Is(got, c.err) {
		t.Fatalf("startupResult(genuine) = %v, want the original error %v", got, c.err)
	}
	// The failure ran exactly once and was not retried by the classification.
	if c.calls != 1 {
		t.Fatalf("EnsureGroup calls = %d, want 1", c.calls)
	}
}

// TestEnsureGroupSuccessPassesThrough proves a successful group creation is a
// plain nil, unaffected by the classification.
func TestEnsureGroupSuccessPassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &fakeEnsurer{}
	if err := ensureGroup(context.Background(), c, logger); err != nil {
		t.Fatalf("ensureGroup(nil) = %v, want nil", err)
	}
	if got := startupResult(nil); got != nil {
		t.Fatalf("startupResult(nil) = %v, want nil", got)
	}
}

// TestStartupCleanupFailureLogLevel proves the startup cleanup failures are
// logged at Warn for a genuine error but demoted to Debug when the lifecycle is
// being cancelled, so an interrupted sweep does not pollute the shutdown trace
// with warnings. The message text is identical in both cases.
func TestStartupCleanupFailureLogLevel(t *testing.T) {
	const msg = "Image cleanup: startup sweep failed"

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Interrupted by shutdown: Debug only.
	logStartupCleanupFailure(ctx, logger, msg, fmt.Errorf("sweep: %w", context.Canceled))
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("interrupted cleanup logged at WARN; want DEBUG:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "level=DEBUG") || !strings.Contains(buf.String(), msg) {
		t.Errorf("interrupted cleanup missing DEBUG log %q:\n%s", msg, buf.String())
	}

	// Genuine failure on a live lifecycle: Warn.
	buf.Reset()
	logStartupCleanupFailure(context.Background(), logger, msg, errors.New("docker unavailable"))
	if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), msg) {
		t.Errorf("genuine cleanup failure missing WARN log %q:\n%s", msg, buf.String())
	}
}

// TestStartupHousekeepingCancelledLifecycleSkipsSweepsAndLogsDebug drives the
// existing startStartupHousekeeping seam with a cancelled lifecycle and a
// barrier that reports ctx.Err(), pinning the async startup-cleanup shutdown
// contract: no sweep runs against a shutting-down world, and the stop is logged
// at Debug (not a barrier-failure Warn). This uses the real seam the worker
// wires, so it exercises the production path without Docker or Redis.
func TestStartupHousekeepingCancelledLifecycleSkipsSweepsAndLogsDebug(t *testing.T) {
	buf := &testutil.SyncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()

	var ran []string
	done := startStartupHousekeeping(lifecycle, logger, startupHousekeeper{
		exclusive: func(ctx context.Context, _ func(context.Context)) error {
			return ctx.Err()
		},
		sweep:  func(context.Context) { ran = append(ran, "sweep") },
		images: func(context.Context) { ran = append(ran, "images") },
		deps:   func(context.Context) { ran = append(ran, "deps") },
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not return on a cancelled lifecycle")
	}
	if len(ran) != 0 {
		t.Fatalf("sweeps ran on a cancelled lifecycle: %v", ran)
	}
	if strings.Contains(buf.String(), "barrier failed") {
		t.Errorf("cancelled lifecycle logged a barrier failure:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "housekeeping stopped by shutdown") {
		t.Errorf("expected the Debug 'stopped by shutdown' line, got:\n%s", buf.String())
	}
}

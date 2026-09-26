//go:build integration

// This file exercises the worker's graceful-shutdown path against real
// external services: real Docker execution (an in-flight handler container)
// and real Redis (PEL state, invocation keys, SQLite flush).
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES both a reachable
// Docker daemon AND Redis at REDIS_TEST_ADDR (default localhost:6379, matching
// compose.dev.yaml); a missing dependency fails the affected tests rather than
// skipping them. Start the documented dev dependencies with
// `docker compose -f compose.dev.yaml up -d`. The Docker daemon is located via
// client.FromEnv, so DOCKER_HOST, the local socket, and a socket proxy are all
// respected.
package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// workerConfig carries the test-controlled constants that replace the package
// CONSTANTS (function.Dir, state.DBPath) and the env-derived values in Run().
// The worker binary reads the literal "/functions" and "/var/lib/relay/...",
// which are not writable on the host, so the test forks the wiring IN-PROCESS
// with these values instead (the same pattern TestIntegrationMetricsEndpoint
// uses, but with the FULL wiring including the real Consume loop and real Docker
// execution).
type workerConfig struct {
	redisURI     string
	stream       string
	group        string
	consumerName string
	fnRoot       string
	statePath    string
	metricsAddr  string
}

// workerEnv is the running worker wiring plus the handles the test needs to
// drive and assert it.
type workerEnv struct {
	ctx          context.Context
	cancel       func()
	consumeDone  chan error
	buf          *testutil.SyncBuffer
	m            *metrics.Registry
	st           *state.State
	client       *redis.Client
	stream       string
	group        string
	consumerName string
	metricsDone  chan error
	statsDone    chan struct{}
} // startWorker replicates the ordering of Run() (internal/worker/worker.go) with
// test-controlled constants, but WITHOUT signal.NotifyContext: the test owns
// ctx/cancel, and cancel() is the exact runtime effect of SIGTERM (NotifyContext
// cancels its ctx when the signal arrives). Sending the real signal to the test
// process is not possible safely, so cancelling the NotifyContext-equivalent ctx
// is the faithful substitute.
//
// Consume runs in a goroutine writing to consumeDone so the test can observe
// when it returns after cancel, exactly as Run()'s blocking Consume would return
// on SIGTERM.
func startWorker(t *testing.T, cfg workerConfig) *workerEnv {
	t.Helper()
	buf := &testutil.SyncBuffer{}
	// DEBUG level: the mid-handler shutdown path logs its cancellation line at
	// Debug (expected shutdown coordination detail), and the assertions below
	// match it.
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	client := redis.NewClient(&redis.Options{Addr: cfg.redisURI})
	t.Cleanup(func() { _ = client.Close() })

	m := metrics.New()

	loader := function.NewLoader(cfg.fnRoot, logger)
	functions, err := loader.Load()
	if err != nil {
		t.Fatalf("load functions: %v", err)
	}

	st, err := state.Open(cfg.statePath)
	if err != nil {
		t.Fatalf("state open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.RebuildFromFS(cfg.fnRoot); err != nil {
		t.Fatalf("state rebuild: %v", err)
	}
	st.PruneRemoved(cfg.fnRoot)
	for _, fn := range functions {
		st.RecordDiscovered(fn)
	}
	restorePersistedStats(m, st)

	manager, err := runtime.NewManager(logger, m, cfg.consumerName)
	if err != nil {
		t.Fatalf("runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	// Conservative startup orphan sweep, hostname-scoped so it never touches the
	// real relay container (hostname "relay-relay") or another worker's.
	sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, _ = manager.SweepOrphanContainers(sweepCtx, cfg.consumerName)
	sweepCancel()

	var prepared []*runner.PreparedFunction
	fingerprints := make(map[string]string, len(functions))
	for _, fn := range functions {
		p, err := manager.Prepare(context.Background(), fn)
		if err != nil {
			t.Fatalf("prepare %s: %v", fn.Name, err)
		}
		fingerprints[fn.Name] = p.Fingerprint
		st.RecordReconcileSuccess(fn.Name, p.Image, p.Fingerprint, time.Now(), fn)
		prepared = append(prepared, runner.NewPrepared(fn, p, manager))
	}

	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:   client,
		Stream:   cfg.stream,
		Group:    cfg.group,
		Consumer: cfg.consumerName,
		Log:      logger,
		Metrics:  m,
		// A short XREADGROUP BLOCK (the production default is 5s): go-redis's
		// blocking read is not interrupted by ctx cancellation, so the read loop
		// observes shutdown only on the next block expiry. The message still has
		// to arrive and be delivered to the handler before the test cancels; the
		// shutdown path (container killed, message left pending, clean Consume
		// return) is identical either way, and the block is purely a read-poll
		// cadence. 250ms keeps the test's shutdown wait bounded without any
		// behavioral change to what is proven.
		Block: 250 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	// Tear the wiring down on EVERY exit path, including a t.Fatalf below
	// (EnsureGroup/Prepare failures): cleanup cancels ctx so the MetricsLogger,
	// metrics server, statsLoop, reconciler, and consumer goroutines stop and
	// join instead of leaking past the test. The test's own cancel() later is
	// the normal (SIGTERM-equivalent) shutdown path.
	t.Cleanup(cancel)

	// Start the metrics components the same way the worker does: named
	// variables, started separately, so the lifecycle reads the same as
	// production (create → start → stop on shutdown).
	metricsInstance := m
	metricsLogger := metrics.NewMetricsLogger(metricsInstance, 30*time.Second, func(format string, args ...any) {
		logger.Debug(fmt.Sprintf(format, args...))
	})
	go metricsLogger.Start(ctx)

	// The metrics server binds synchronously in Start (fail-fast on a taken
	// port) and serves in the background. On shutdown (ctx cancel) Stop performs
	// the bounded graceful close; the bounded context keeps a wedged handler
	// from hanging the test. metricsDone mirrors the old "server exited" signal
	// so the test can assert it stops promptly on cancel.
	metricsServer := metrics.NewServer(cfg.metricsAddr, metricsInstance.Handler(), logger)
	if err := metricsServer.Start(); err != nil {
		t.Fatalf("Metrics server start: %v", err)
	}
	metricsDone := make(chan error, 1)
	go func() {
		<-ctx.Done()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		metricsDone <- metricsServer.Stop(stopCtx)
	}()

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		statsLoop(ctx, newStatsFlusher(st, m), statsFlushInterval)
	}()

	if err := consumer.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	runWorker := runner.NewWithMetrics(prepared, logger, m)
	runWorker.SetHostname(cfg.consumerName)
	runWorker.SetMaxHandlerTimeout(stream.MaxRuleTimeout)

	rec := reconciler.New(
		reconciler.Config{
			Root:           cfg.fnRoot,
			State:          st,
			Retire:         func(_ string, oldImage string) { runWorker.RetireImage(oldImage) },
			RemoveFunction: runWorker.RemoveFunctionImages,
		},
		runWorker.Registry(),
		manager,
		logger,
	)
	for _, fn := range functions {
		rec.Seed(fn, fingerprints[fn.Name])
	}
	go rec.Start(ctx)

	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.Consume(ctx, runWorker.Handle)
	}()

	return &workerEnv{
		ctx: ctx, cancel: cancel, consumeDone: consumeDone,
		buf: buf, m: m, st: st, client: client,
		stream: cfg.stream, group: cfg.group, consumerName: cfg.consumerName,
		metricsDone: metricsDone, statsDone: statsDone,
	}
}

// xadd appends an event to the stream and returns the message ID.
func xadd(t *testing.T, cli *redis.Client, stream, event string) string {
	t.Helper()
	id, err := cli.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"event": event},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	return id
}

// pendingEntry returns the XPendingExt entry for id, or (zero, false) if absent.
func pendingEntry(cli *redis.Client, stream, group, id string) (redis.XPendingExt, bool) {
	entries, err := cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		return redis.XPendingExt{}, false
	}
	for _, pe := range entries {
		if pe.ID == id {
			return pe, true
		}
	}
	return redis.XPendingExt{}, false
}

// findContainerByLabels returns the ID of the first container (running or
// exited) carrying every label pair, or "" when none matches. It mirrors the
// sweep's own client-side label predicate.
func findContainerByLabels(ctx context.Context, cli *client.Client, labels map[string]string) string {
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return ""
	}
	for _, c := range list.Items {
		match := true
		for k, v := range labels {
			if c.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return c.ID
		}
	}
	return ""
}

// waitForContainer polls until a container carrying every label appears,
// returning its ID. It is the synchronization point for "the handler is actually
// executing" — no arbitrary sleep. The bound is generous (45s) because on a
// cold daemon the first node24 build inside startWorker's Prepare can dominate;
// the container only appears once the image exists and the event is delivered.
func waitForContainer(t *testing.T, cli *client.Client, labels map[string]string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for {
		if id := findContainerByLabels(ctx, cli, labels); id != "" {
			return id
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for container with labels %v", labels)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitForContainerGone polls until no container carries every label, or the
// deadline passes. AutoRemove removes the container asynchronously after the
// process stops, so callers must await removal rather than assert it at the
// instant Execute returns.
func waitForContainerGone(t *testing.T, cli *client.Client, labels map[string]string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if findContainerByLabels(ctx, cli, labels) == "" {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// writeTestFile writes a file into dir, failing the test on error.
func writeTestFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// TestIntegrationGracefulShutdownMidHandler is the end-to-end graceful-shutdown
// test: a real worker (full wiring: real Consume loop, real Docker execution,
// real Redis PEL, real SQLite flush) is cancelled mid-handler, and the test
// asserts a clean bounded shutdown: the in-flight container is removed, the
// message is left pending (not acked), the invocation is not marked complete,
// the final SQLite flush runs within its bound, and the message is recoverable
// through the normal reclaim path by a second consumer.
func TestIntegrationGracefulShutdownMidHandler(t *testing.T) {
	testutil.RequireRedis(t)
	dcli := testutil.RequireDocker(t)

	redisURI := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	prefix := fmt.Sprintf("shutdown-itest-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	groupName := prefix + "-group"
	// The consumer name must be UNIQUE so the orphan sweep and label filters
	// never touch the real relay container (hostname "relay-relay").
	consumerName := prefix + "-consumer"

	// Free metrics port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

	fnRoot := t.TempDir()
	statePath := filepath.Join(t.TempDir(), "db.sqlite3")

	// Derive a unique, length-capped function name from the test name + a
	// nanosecond stamp so concurrent runs on one daemon cannot collide on the
	// execution container labels or the function directory. testutil.UniqueName
	// guarantees the name stays within function.ValidName's 63-char cap even for
	// a long test name.
	fnName := testutil.UniqueName(t, "shutdown")

	// Write the function. The handler sleeps 30s while the rule timeout is 25s,
	// so TryStart persists a running deadline (now+25s) that comfortably exceeds
	// the shutdown point — the handler is genuinely mid-execution when we cancel.
	fnDir := filepath.Join(fnRoot, fnName)
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatalf("mkdir fn: %v", err)
	}
	writeTestFile(t, fnDir, "template.yaml", `
runtime: node24
events:
  - handler: index.slow
    pattern:
      event_name: [INSERT]
    timeout: 25s
`)
	writeTestFile(t, fnDir, "index.js", `
export async function slow(event) {
  console.log("slow started " + event.event_id);
  await new Promise(r => setTimeout(r, 30000));
  console.log("slow done");
}
`)

	env := startWorker(t, workerConfig{
		redisURI: redisURI, stream: streamName, group: groupName,
		consumerName: consumerName, fnRoot: fnRoot, statePath: statePath,
		metricsAddr: metricsAddr,
	})

	// XADD an event matching the rule.
	msgID := xadd(t, env.client, streamName, `{"event_id":"evt_shutdown","event_name":"INSERT"}`)

	// Synchronize on the handler actually executing: poll for the execution
	// container carrying the identity labels. (relay.handler is EMPTY at
	// creation on the reused execution container — per-invocation attribution
	// lives in the request frame and the output prefix — so the identity label
	// set is the stable wait predicate.)
	containerID := waitForContainer(t, dcli, map[string]string{
		"relay.function": fnName,
		"relay.hostname": consumerName,
	})
	t.Logf("in-flight container %s observed", containerID)

	// The message is in the PEL (delivered to consumer A) with retry count 1.
	testutil.WaitFor(t, 8*time.Second, "message in PEL", func() bool {
		_, ok := pendingEntry(env.client, streamName, groupName, msgID)
		return ok
	})
	if pe, ok := pendingEntry(env.client, streamName, groupName, msgID); !ok || pe.RetryCount != 1 {
		t.Fatalf("pending entry = %+v, ok=%v; want retry count 1", pe, ok)
	}

	// Cancel the worker ctx — the exact runtime effect of SIGTERM (NotifyContext
	// cancels its ctx when the signal arrives). The in-flight handler's ctx is
	// derived from this one, so the executor kills the container and returns a
	// ctx.Err-wrapped error; the runner's failure path calls EndRunning; and
	// processMessage sees ctx.Err() != nil and leaves the message pending.
	shutdownStart := time.Now()
	env.cancel()

	// Assert Consume returns within a hard bound. The XREADGROUP Block is short
	// (250ms, set on the consumer above), so it returns promptly on cancel; the
	// remainder is the in-flight container kill.
	var consumeErr error
	select {
	case consumeErr = <-env.consumeDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Consume did not return within 15s of cancel")
	}
	shutdownElapsed := time.Since(shutdownStart)
	if consumeErr != nil {
		t.Fatalf("consume returned error: %v", consumeErr)
	}
	if shutdownElapsed > 15*time.Second {
		t.Fatalf("shutdown took %v, want < 15s", shutdownElapsed)
	}
	t.Logf("Consume returned cleanly after %v", shutdownElapsed)

	// The stats-flush shutdown step runs after Consume returns, exactly like
	// Run(). The shutdown registry owns the step's bound (2s here), so apply it
	// the same way and assert the flush completes within it with a stats row.
	flushStart := time.Now()
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
	finalStatsFlush(flushCtx, newStatsFlusher(env.st, env.m))
	flushCancel()
	flushElapsed := time.Since(flushStart)
	if flushElapsed > 2*time.Second {
		t.Fatalf("finalStatsFlush took %v, want < 2s", flushElapsed)
	}
	if _, ok := env.st.Stats(); !ok {
		t.Fatal("expected stats row after finalStatsFlush")
	}

	// The metrics server and stats loop must exit promptly on cancel.
	select {
	case err := <-env.metricsDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("Metrics server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Metrics server did not stop on cancel")
	}
	select {
	case <-env.statsDone:
	case <-time.After(5 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}

	// Docker cleanup completed: the in-flight container is gone (AutoRemove after
	// the kill, plus the deferred best-effort remove).
	if !waitForContainerGone(t, dcli, map[string]string{
		"relay.function": fnName,
		"relay.hostname": consumerName,
	}) {
		t.Error("in-flight container should have been removed after shutdown")
	}

	// The message is NOT acked: still present in the PEL.
	if _, ok := pendingEntry(env.client, streamName, groupName, msgID); !ok {
		t.Fatal("message should still be pending (not acked) after shutdown")
	}

	// Invocation state is NOT "ok": the invocation did not complete. In the
	// graceful-shutdown path the runner's failure branch calls EndRunning, which
	// HDELs the running marker, so the field is typically absent; a hard crash
	// would leave it as "running:<deadline>". Either way it must not be "ok".
	invKey := "relay:invocation:" + streamName + ":" + groupName + ":" + msgID
	if v, err := env.client.HGet(context.Background(), invKey, fnName+"/index.slow").Result(); err == nil && v == "ok" {
		t.Fatal("invocation state should NOT be 'ok' after mid-handler shutdown")
	}

	// The stream layer must log the shutdown-cancel path. The full message is
	// "Message %q: handler canceled during shutdown; leaving pending" — the
	// sentence-case convention capitalizes only the first word (Message), so
	// "handler" mid-sentence stays lowercase; assert the mid-sentence tail.
	if !strings.Contains(env.buf.String(), "handler canceled during shutdown; leaving pending") {
		t.Error("expected 'Message ...: handler canceled during shutdown; leaving pending' log line")
	}

	// Recovery: a SECOND consumer (new name, same stream/group) with fast reclaim
	// settings reclaims the idle pending message and acks it, proving the message
	// is recoverable through the normal reclaim path after a crash-like shutdown.
	//
	// Invocation-state deletion rationale: after a HARD crash the running marker
	// survives as "running:<deadline>" (now+25s), and the second consumer's
	// runner would skip the invocation until that deadline expires — the deadline
	// model's crash-recovery latency. In THIS graceful-shutdown test the runner's
	// failure path already cleared the marker via EndRunning, so the field is
	// absent; the DEL below is therefore a defensive no-op that also covers the
	// crash case (simulating the running deadline having expired) so the second
	// consumer executes promptly without a 25s wait. This models the crash
	// semantics exactly (the marker survives and must be waited out) without
	// stretching the test.
	_ = env.client.Del(context.Background(), invKey).Err()

	var bCalls atomic.Int64
	bCtx, bCancel := context.WithCancel(context.Background())
	bConsumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:          env.client,
		Stream:          streamName,
		Group:           groupName,
		Consumer:        consumerName + "-b",
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
	})
	if err := bConsumer.EnsureGroup(bCtx); err != nil {
		t.Fatalf("ensure group B: %v", err)
	}
	bDone := make(chan error, 1)
	go func() {
		bDone <- bConsumer.Consume(bCtx, func(ctx context.Context, mid string, ev map[string]any) error {
			if mid == msgID {
				bCalls.Add(1)
			}
			return nil
		})
	}()

	// Wait for the message to be reclaimed and acked (gone from the PEL).
	testutil.WaitFor(t, 8*time.Second, "message reclaimed and acked by consumer B", func() bool {
		_, ok := pendingEntry(env.client, streamName, groupName, msgID)
		return !ok
	})
	if got := bCalls.Load(); got != 1 {
		t.Fatalf("consumer B handler ran %d times, want exactly 1", got)
	}

	// Stop consumer B and clean up the stream/DLQ/invocation keys.
	bCancel()
	select {
	case <-bDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer B did not stop")
	}
	_ = env.client.Del(context.Background(), streamName, stream.DLQStreamFor(streamName), invKey).Err()
}

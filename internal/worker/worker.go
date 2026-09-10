// Package worker is the long-running Relay runtime, started via `relay start`.
// It loads functions, builds their images, reconciles them live, and consumes
// the Redis stream, blocking until signalled. Configuration comes entirely
// from the environment. It also exposes a Prometheus /metrics endpoint (see
// internal/metrics) and flushes the registry into the local state database on
// a fixed 5-second cadence: stats accumulate in memory (the registry is the
// single source of truth), Prometheus reflects them immediately, and SQLite
// receives the current absolute snapshot every interval.
package worker

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/secrets"
	"relay/internal/state"
	"relay/internal/stream"
)

// statsFlushInterval is the fixed SQLite snapshot cadence. Telemetry, not
// event-processing state: Prometheus stays live in-process, while SQLite
// receives the current absolute snapshot every interval. Deliberately NOT
// env/flag-configurable — a fixed cadence keeps the durability model simple
// (a hard crash loses at most one interval of telemetry).
const statsFlushInterval = 5 * time.Second

// Run wires the whole worker: startup state, then the reconciler and stream
// consumer. It blocks in Consume until the process is signalled.
func Run(logger *log.Logger) {
	cfg := struct {
		redisAddr   string
		redisStream string
		redisGroup  string
		redisCons   string
	}{
		redisAddr:   config.MustEnv("REDIS_ADDR"),
		redisStream: config.MustEnv("REDIS_STREAM"),
		redisGroup:  config.MustEnv("REDIS_GROUP"),
	}

	// Resolve the consumer identity explicitly so startup fails fast with a
	// clear message when the hostname is unavailable or empty.
	consumerName, err := config.ConsumerName()
	if err != nil {
		logger.Fatalf("consumer identity: %v", err)
	}
	cfg.redisCons = consumerName

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer client.Close()

	// The metrics registry is wired into the runner, the runtime manager, and
	// the stream consumer. It is nil-safe throughout, so observability can never
	// break processing. It is process-lifetime-scoped: its counters start at 0,
	// so after the state DB opens below we seed it from the persisted cumulative
	// snapshot (see restorePersistedStats) before the snapshot loop starts.
	m := metrics.New()

	loader := function.NewLoader(function.Dir, logger)
	functions, err := loader.Load()
	if err != nil {
		logger.Fatalf("load functions: %v", err)
	}
	logger.Printf("loaded %d function(s) from %s", len(functions), function.Dir)

	// The state database is a read-only local state view (see internal/state).
	// It is NOT the source of truth and never drives matching or building. Open
	// recreates a missing DB; RebuildFromFS repopulates an empty one from
	// /functions; then startup discovery records each loaded function. All
	// state errors are logged and non-fatal — Relay runs without the state DB
	// if it is broken.
	st, err := state.Open(state.DBPath)
	if err != nil {
		logger.Printf("state: open (continuing without): %v", err)
		st = nil
	}
	if st != nil {
		defer st.Close()
		if err := st.RebuildFromFS(function.Dir); err != nil {
			logger.Printf("state: rebuild from fs (continuing): %v", err)
		}
		// Prune state rows for functions that no longer exist on disk. This
		// must run BEFORE restorePersistedStats so a pruned function's
		// function_stats row is gone before the fresh registry is seeded from
		// it: a function removed while this worker was down must not be re-seeded
		// into metrics.
		st.PruneRemoved(function.Dir)
		for _, fn := range functions {
			st.RecordDiscovered(fn)
		}
	}

	// Restore the persisted cumulative counters into the fresh registry before
	// the snapshot loop starts. The registry only knows this process's lifetime,
	// while the SQLite stats rows are cumulative history; seeding bridges them so
	// the immediate first snapshot (see statsLoop) writes back the restored
	// values instead of zeroing the persisted totals. Gauges are deliberately
	// NOT restored — they are point-in-time backlog snapshots refreshed each
	// snapshot.
	restorePersistedStats(m, st)

	// Prepare (build) each function's image. A function whose image cannot be
	// built is marked unavailable so the runner skips it; the rest continue.
	manager, err := runtime.NewManager(logger, m, consumerName)
	if err != nil {
		logger.Fatalf("runtime: %v", err)
	}
	defer manager.Close()

	// Conservative startup orphan sweep. Before any function is prepared or any
	// execution container is created, remove execution containers left behind by
	// a previous Relay process on THIS hostname (a crash mid-invocation, or a
	// never-exited container). It is label- and hostname-scoped, so other
	// workers' containers and non-Relay containers are never touched. A bounded
	// context guarantees the sweep can never hang startup; on timeout or error
	// we log and continue, leaving the orphans for a later restart.
	sweepCtx, sweepCancel := context.WithTimeout(context.Background(), 30*time.Second)
	n, sweepErr := manager.SweepOrphanContainers(sweepCtx, consumerName)
	sweepCancel()
	if sweepErr != nil {
		logger.Printf("startup: orphan container sweep: %v", sweepErr)
	} else if n > 0 {
		logger.Printf("startup: removed %d orphan container(s) from a previous relay process", n)
	}
	preparedCount := 0
	var prepared []*runner.PreparedFunction
	for _, fn := range functions {
		p, err := manager.Prepare(context.Background(), fn)
		if err != nil {
			logger.Printf("function %q: prepare: %v", fn.Name, err)
			if st != nil {
				st.RecordReconcileFailure(fn.Name, err)
			}
			prepared = append(prepared, runner.NewUnavailable(fn))
			continue
		}
		if st != nil {
			// Fingerprint may differ from the discovery-time value if content
			// changed between load and build; the state DB records the final state.
			fp, fperr := function.Fingerprint(fn.Dir)
			if fperr != nil {
				logger.Printf("function %q: fingerprint: %v", fn.Name, fperr)
				fp = ""
			}
			st.RecordReconcileSuccess(fn.Name, p.Image, fp, time.Now(), fn)
		}
		prepared = append(prepared, runner.NewPrepared(fn, p, manager))
		preparedCount++
	}
	logger.Printf("prepared %d function(s)", preparedCount)

	// Conservative startup image sweep. After every current function's image is
	// built (reused if unchanged), remove Relay-owned images that no longer
	// correspond to a live function version: superseded versions of current
	// functions and versions of functions deleted while the worker was down.
	//
	// The keep set holds (a) each current function's expected fingerprinted
	// image and (b) any last-active image the state DB recorded for a function
	// that is still on disk. The latter guards the race where state recorded an
	// image just before a swap that has not yet landed here (e.g. a crash
	// between reg.Replace and RecordReconcileSuccess): the recorded image may
	// still be the one serving, so it is never removed even if its fingerprint
	// no longer matches. Images belonging to names absent from both /functions
	// AND state (the DB pruned them earlier in startup) are genuinely removed
	// and dropped. When state is nil (DB failed to open) we cannot distinguish
	// a removed function from a mis-fingerprinted one, so orphan removal is
	// skipped entirely and only the (self-evidently current) prepared images
	// are kept; this is conservative: nothing is removed that might still serve.
	keep := make(map[string]bool)
	for _, fn := range functions {
		if fp, err := function.Fingerprint(fn.Dir); err == nil {
			keep[runtime.ImageRef(fn.Name, fp)] = true
		}
	}
	// stateSweep is only armed when the DB was available.
	if st != nil {
		for _, fn := range functions {
			if d, ok := st.GetFunction(fn.Name); ok && d.Image != "" {
				keep[d.Image] = true
			}
		}
		if _, err := manager.RemoveImagesExcept(context.Background(), keep); err != nil {
			logger.Printf("image cleanup: startup sweep: %v", err)
		}
	}

	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:   client,
		Stream:   cfg.redisStream,
		Group:    cfg.redisGroup,
		Consumer: cfg.redisCons,
		Log:      logger,
		Metrics:  m,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Expose the metrics snapshot on a fixed interval until shutdown. It runs in
	// its own goroutine and exits when ctx is cancelled.
	go m.LogLoop(ctx, 30*time.Second, logger.Printf)

	// Expose the Prometheus /metrics endpoint. METRICS_ADDR is optional and
	// defaults to the conventional 9090 port. ServeHTTP logs its own bind
	// failures and retries, so a temporarily occupied port heals instead of
	// crashing the worker; it returns nil on a clean shutdown (or ctx.Err on
	// the retry-bind path). It starts before EnsureGroup/Consume so Prometheus
	// can scrape during startup builds.
	metricsAddr := config.Env("METRICS_ADDR", metrics.DefaultAddr)
	logger.Printf("metrics http server listening on %s", metricsAddr)
	go m.ServeHTTP(ctx, metricsAddr, logger.Printf)

	// Flush the registry into the state database on the fixed 5-second cadence.
	// It is nil-safe on both the registry and the state handle and stops when
	// ctx is cancelled.
	go statsLoop(ctx, m, st, statsFlushInterval)

	if err := consumer.EnsureGroup(ctx); err != nil {
		logger.Fatalf("ensure consumer group: %v", err)
	}

	runWorker := runner.NewWithMetrics(prepared, logger, m)
	// Stamp the relay.hostname label (the worker/consumer identity) on every
	// execution container. Must be set before Consume begins; it is wired right
	// after construction so all invocations carry it.
	runWorker.SetHostname(consumerName)
	// Wire the local secrets provider. It is infallible to construct (the
	// directory is created lazily on Set, never on Resolve), and a missing
	// secret surfaces as a per-invocation Resolve error rather than a startup
	// failure — startup does not validate that referenced secrets exist.
	secretProvider, err := secrets.NewLocalProvider(secrets.SecretsDir)
	if err != nil {
		logger.Fatalf("secrets: %v", err)
	}
	runWorker.SetSecretProvider(secretProvider)
	// Cap every rule's handler timeout at the same value template validation
	// enforces (function.MaxTimeout). Defense in depth: a misconfigured or
	// hot-swapped template can never run a handler past the cap. The capped
	// value is the maximum persisted running deadline an invocation can carry
	// (see runner.SetMaxHandlerTimeout / stream.InvocationState.TryStart);
	// template validation enforces it at load.
	runWorker.SetMaxHandlerTimeout(stream.MaxRuleTimeout)

	// Watch /functions and reconcile functions live: rebuild changed images,
	// discover new ones, drop removed ones. The runner's registry is swapped
	// atomically behind the snapshots the consumer already uses. The retire
	// hooks hand superseded function images back to the runner so it can remove
	// them once no in-flight execution uses them.
	rec := reconciler.New(
		reconciler.Config{
			Root:           function.Dir,
			State:          st,
			Retire:         func(_ string, oldImage string) { runWorker.RetireImage(oldImage) },
			RemoveFunction: runWorker.RemoveFunctionImages,
		},
		runWorker.Registry(),
		manager,
		logger,
	)
	for _, fn := range functions {
		rec.Seed(fn)
	}
	logger.Printf("watching %s for changes", function.Dir)

	// Runs in its own goroutine and stops when ctx is cancelled.
	go rec.Start(ctx)

	logger.Printf("consuming stream %q as group %q consumer %q",
		cfg.redisStream,
		cfg.redisGroup,
		cfg.redisCons,
	)
	if err := consumer.Consume(ctx, runWorker.Handle); err != nil {
		logger.Fatalf("consume: %v", err)
	}

	// Final flush of the registry into SQLite before the deferred st.Close()
	// runs. Bounded by a short timeout so a wedged SQLite cannot hang shutdown;
	// failure is logged and shutdown continues (telemetry, not state).
	finalStatsFlush(m, st)

	logger.Printf("shutdown complete")
}

// restorePersistedStats seeds the fresh process-lifetime metrics registry with
// the cumulative counters persisted in the state database, so the first
// snapshot never resets them. The registry only knows this process's lifetime,
// while the SQLite stats rows are cumulative history; seeding bridges them. It
// is nil-safe on both the registry and the state handle (a nil st means the DB
// failed to open, so there is nothing to restore). Gauges are deliberately NOT
// restored: they are point-in-time backlog snapshots refreshed each snapshot.
func restorePersistedStats(m *metrics.Registry, st *state.State) {
	if m == nil || st == nil {
		return
	}
	if gs, ok := st.Stats(); ok {
		m.SeedCounter("events_processed_total", gs.EventsProcessedTotal)
		m.SeedCounter("handler_success_total", gs.HandlerSuccessTotal)
		m.SeedCounter("handler_failure_total", gs.HandlerFailureTotal)
		m.SeedCounter("retries_total", gs.RetryTotal)
		m.SeedCounter("dlq_entries_total", gs.DLQTotal)
	}
	for _, fs := range st.AllFunctionStats() {
		m.SeedFunctionStat(metrics.FunctionStat{
			Function:            fs.Function,
			Events:              fs.EventsProcessedTotal,
			HandlerSuccessTotal: fs.HandlerSuccessTotal,
			HandlerFailureTotal: fs.HandlerFailureTotal,
			RetriesTotal:        fs.RetryTotal,
			DLQTotal:            fs.DLQTotal,
		})
	}
}

// snapshotStats maps the metrics registry into the state database's Stats row.
// The registry counter names feed the SQLite columns directly, with one rename
// (retries_total → RetryTotal) and the float gauges truncated to int64. It is
// nil-safe: a nil registry yields a zero Stats so the snapshot path can never
// panic or block processing.
func snapshotStats(m *metrics.Registry) state.Stats {
	if m == nil {
		return state.Stats{}
	}
	return state.Stats{
		EventsProcessedTotal:    m.Counter("events_processed_total"),
		HandlerSuccessTotal:     m.Counter("handler_success_total"),
		HandlerFailureTotal:     m.Counter("handler_failure_total"),
		RetryTotal:              m.Counter("retries_total"),
		DLQTotal:                m.Counter("dlq_entries_total"),
		PendingEntries:          int64(m.Gauge("pending_entries")),
		OldestPendingAgeSeconds: int64(m.Gauge("pending_oldest_age_seconds")),
	}
}

// funcSnapshotStats maps the registry's per-function counters into the state
// layer's FunctionStats rows. It is nil-safe: a nil registry yields an empty
// slice so the snapshot path can never panic or block processing.
func funcSnapshotStats(m *metrics.Registry) []state.FunctionStats {
	if m == nil {
		return nil
	}
	stats := m.FunctionStatsSnapshot()
	out := make([]state.FunctionStats, 0, len(stats))
	for _, fs := range stats {
		out = append(out, state.FunctionStats{
			Function:             fs.Function,
			EventsProcessedTotal: fs.Events,
			HandlerSuccessTotal:  fs.HandlerSuccessTotal,
			HandlerFailureTotal:  fs.HandlerFailureTotal,
			RetryTotal:           fs.RetriesTotal,
			DLQTotal:             fs.DLQTotal,
		})
	}
	return out
}

// statsLoop is the flush loop: it mirrors the in-memory metrics registry into
// the state database on a fixed interval until ctx is cancelled. The first
// flush runs immediately so the stats row exists before the first tick (this
// also makes `relay stats` useful right after startup). It is nil-safe on both
// the registry and the state handle, so observability can never break
// processing. The loop's ctx is the shutdown ctx; periodic flushes use it
// directly (a cancelled ctx simply stops the loop).
func statsLoop(ctx context.Context, m *metrics.Registry, st *state.State, interval time.Duration) {
	if st == nil {
		<-ctx.Done()
		return
	}
	recordSnapshots(ctx, st, m)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			recordSnapshots(ctx, st, m)
		}
	}
}

// finalStatsFlush performs a bounded final flush of the registry into SQLite on
// graceful shutdown, so the last interval of telemetry is not lost. It is
// bounded by a short timeout so a wedged SQLite cannot hang shutdown; on
// timeout or error it logs and returns (telemetry, not state). It is nil-safe
// on both the registry and the state handle.
func finalStatsFlush(m *metrics.Registry, st *state.State) {
	if m == nil || st == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	recordSnapshots(ctx, st, m)
}

// recordSnapshots writes the whole stats snapshot — the global stats row and
// one function_stats row per function with any attributed activity — in a
// single short transaction (see state.RecordStatsSnapshot). The transaction
// first prunes orphaned function_stats rows (a removed function's row is not
// re-created even though its registry counters linger until process restart:
// the registry is the in-memory store and we deliberately do not delete its
// counters on removal — the flush simply stops persisting the removed function
// because its functions row is gone). Per-function writes are bounded by the
// function count, so the 5s cadence keeps them small. A failed flush is logged
// and retried next tick with the current absolute values; no path resets
// counters on failure.
func recordSnapshots(ctx context.Context, st *state.State, m *metrics.Registry) {
	// RecordStatsSnapshot logs internally on error (matching the state package's
	// non-fatal style); the returned error is only for the caller to bound the
	// write with a context.
	_ = st.RecordStatsSnapshot(ctx, snapshotStats(m), funcSnapshotStats(m))
}

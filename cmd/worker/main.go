// relay-worker is the long-running Relay process. It loads functions, builds
// their images, reconciles them live, and consumes the Redis stream, blocking
// until signalled. Configuration comes entirely from the environment. It also
// exposes a Prometheus /metrics endpoint (see internal/metrics) and periodically
// snapshots the registry into the local state database.
package main

import (
	"context"
	"log"
	"os"
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
	"relay/internal/state"
	"relay/internal/stream"
)

// statsSnapshotInterval is how often the worker maps the metrics registry into
// the state database's stats row. It aligns with the metrics
// LogLoop interval so both observability views refresh on the same cadence.
const statsSnapshotInterval = 30 * time.Second

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	run(logger)
}

// run wires the whole worker: startup state, then the reconciler and stream
// consumer. It blocks in Consume until the process is signalled.
func run(logger *log.Logger) {
	cfg := struct {
		redisAddr   string
		redisStream string
		redisGroup  string
		redisCons   string
	}{
		redisAddr:   config.MustEnv("REDIS_ADDR"),
		redisStream: config.MustEnv("REDIS_STREAM"),
		redisGroup:  config.MustEnv("REDIS_GROUP"),
		redisCons:   config.MustEnv("REDIS_CONSUMER"),
	}

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
	manager, err := runtime.NewManager(logger, m)
	if err != nil {
		logger.Fatalf("runtime: %v", err)
	}
	defer manager.Close()
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

	// Snapshot the registry into the state database on the same cadence as the
	// metrics LogLoop. It is nil-safe on both the registry and the state handle
	// and stops when ctx is cancelled.
	go statsLoop(ctx, m, st, statsSnapshotInterval)

	if err := consumer.EnsureGroup(ctx); err != nil {
		logger.Fatalf("ensure consumer group: %v", err)
	}

	runWorker := runner.NewWithMetrics(prepared, logger, m)

	// Watch /functions and reconcile functions live: rebuild changed images,
	// discover new ones, drop removed ones. The runner's registry is swapped
	// atomically behind the snapshots the consumer already uses.
	rec := reconciler.New(
		reconciler.Config{Root: function.Dir, State: st},
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

// statsLoop snapshots the registry into the state database on a fixed interval
// until ctx is cancelled. The first snapshot runs immediately so the
// stats row exists before the first tick. It is nil-safe on both the
// registry and the state handle, so observability can never break processing.
func statsLoop(ctx context.Context, m *metrics.Registry, st *state.State, interval time.Duration) {
	if st == nil {
		<-ctx.Done()
		return
	}
	recordSnapshots(st, m)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			recordSnapshots(st, m)
		}
	}
}

// recordSnapshots writes the global stats row and one function_stats row per
// function with any attributed activity. Per-function writes are bounded by the
// function count, so the 30s cadence keeps them small.
func recordSnapshots(st *state.State, m *metrics.Registry) {
	st.RecordStats(snapshotStats(m))
	for _, fs := range funcSnapshotStats(m) {
		st.RecordFunctionStats(fs)
	}
}

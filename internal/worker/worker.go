// Package worker is the long-running Relay runtime, started via `relay start`.
// It loads functions, builds their images, reconciles them live, and consumes
// the Redis stream, blocking until signalled. Configuration comes entirely
// from the environment. It can also expose a Prometheus /metrics endpoint (see
// internal/metrics), gated on the METRICS_ADDR environment variable, and
// flushes the registry into the local state database on a fixed 5-second
// cadence: stats accumulate in memory (the registry is the single source of
// truth), Prometheus reflects them immediately, and SQLite receives the current
// absolute snapshot every interval.
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
	// Load the whole application configuration up front. Load resolves every
	// required and optional variable and returns a Config value, so startup
	// fails fast with a single clear message when anything is missing or
	// malformed (see internal/config). A malformed Redis DSN — a plain address
	// or a redis(s):// URL — still fails fast here with a redacted message
	// rather than silently connecting to the wrong host.
	cfg := config.Load(logger)
	// Resolve the Redis client options from cfg.RedisAddr.
	redisOpts, err := config.RedisOptions(cfg.RedisAddr)
	if err != nil {
		logger.Fatalf("Redis config: %v", err)
	}
	client := redis.NewClient(redisOpts)
	defer client.Close()

	// Metrics are opt-in, gated on METRICS_ADDR (cfg.MetricsAddr): when it is
	// set, the registry instance, the /metrics HTTP server, and the periodic
	// snapshot logger are created, started, and stopped explicitly; when unset,
	// none of them exist and instrumentation continues through the registry's
	// nil-safe no-op methods (a nil *metrics.Registry passed to the runner,
	// runtime manager, stream consumer, and stats flush never breaks
	// processing). One lifecycle rule makes this safe: every consumer of the
	// registry is nil-safe, AND the stats flush is gated alongside it below —
	// a nil registry must never feed snapshotStats, or the 5s flush loop would
	// clobber the persisted cumulative totals with zeros.
	var metricsInstance *metrics.Registry
	var metricsServer *metrics.Server
	if cfg.MetricsAddr != "" {
		metricsInstance = metrics.New()
		metricsServer = metrics.NewServer(
			cfg.MetricsAddr,
			metricsInstance.Handler(),
			logger,
		)
	}

	loader := function.NewLoader(function.Dir, logger)
	functions, err := loader.Load()
	if err != nil {
		logger.Fatalf("Load functions: %v", err)
	}
	logger.Printf("Loaded %d function(s) from %s", len(functions), function.Dir)

	// The state database is a read-only local state view (see internal/state).
	// It is NOT the source of truth and never drives matching or building. Open
	// recreates a missing DB; RebuildFromFS repopulates an empty one from
	// /functions; then startup discovery records each loaded function. All
	// state errors are logged and non-fatal — Relay runs without the state DB
	// if it is broken.
	st, err := state.Open(state.DBPath)
	if err != nil {
		logger.Printf("State: open (continuing without): %v", err)
		st = nil
	}
	if st != nil {
		defer st.Close()
		if err := st.RebuildFromFS(function.Dir); err != nil {
			logger.Printf("State: rebuild from fs (continuing): %v", err)
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
	restorePersistedStats(metricsInstance, st)

	// Prepare (build) each function's image. A function whose image cannot be
	// built is marked unavailable so the runner skips it; the rest continue.
	manager, err := runtime.NewManager(logger, metricsInstance, cfg.ConsumerName)
	if err != nil {
		logger.Fatalf("Runtime: %v", err)
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
	n, sweepErr := manager.SweepOrphanContainers(sweepCtx, cfg.ConsumerName)
	sweepCancel()
	if sweepErr != nil {
		logger.Printf("Startup: orphan container sweep: %v", sweepErr)
	} else if n > 0 {
		logger.Printf("Startup: removed %d orphan container(s) from a previous relay process", n)
	}
	preparedCount := 0
	var prepared []*runner.PreparedFunction
	for _, fn := range functions {
		p, err := manager.Prepare(context.Background(), fn)
		if err != nil {
			logger.Printf("Function %q: prepare: %v", fn.Name, err)
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
				logger.Printf("Function %q: fingerprint: %v", fn.Name, fperr)
				fp = ""
			}
			st.RecordReconcileSuccess(fn.Name, p.Image, fp, time.Now(), fn)
		}
		prepared = append(prepared, runner.NewPrepared(fn, p, manager))
		preparedCount++
	}
	logger.Printf("Prepared %d function(s)", preparedCount)

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
			logger.Printf("Image cleanup: startup sweep: %v", err)
		}
	}

	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:   client,
		Stream:   cfg.RedisStream,
		Group:    cfg.RedisGroup,
		Consumer: cfg.ConsumerName,
		Log:      logger,
		Metrics:  metricsInstance,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start the metrics components created in the gated block above. The
	// logger exposes the registry snapshot on a fixed interval until shutdown
	// (its own goroutine, exits when ctx is cancelled) and reads the same
	// registry the /metrics server serves. The server binds synchronously and
	// returns nil once serving, so it is safe to call here in the startup path;
	// a bind failure (a taken metrics port) returns an error and is FATAL — the
	// worker does not retry a temporarily occupied port, because a
	// metrics-address conflict is a config error that should surface at startup
	// rather than heal invisibly. Both are nil when METRICS_ADDR is unset and
	// are simply not started.
	var metricsLogger *metrics.MetricsLogger
	if metricsInstance != nil {
		metricsLogger = metrics.NewMetricsLogger(
			metricsInstance,
			metrics.DefaultLogInterval,
			logger.Printf,
		)
		go metricsLogger.Start(ctx)

		if err := metricsServer.Start(); err != nil {
			logger.Fatalf("Metrics server: %v", err)
		}
		logger.Printf("Metrics http server listening on %s", cfg.MetricsAddr)
	}

	// Flush the registry into the state database on the fixed 5-second cadence.
	// It is gated on the metrics instance existing: with metrics disabled there
	// is nothing to snapshot, and a nil-registry flush would clobber the
	// persisted cumulative totals with zeros. The loop still parks on ctx so
	// shutdown ordering stays uniform, and it is nil-safe on the state handle.
	if metricsInstance != nil {
		go statsLoop(ctx, metricsInstance, st, statsFlushInterval)
	} else {
		go parkUntilShutdown(ctx)
	}

	// Optional internal stream retention (cfg.StreamRetention from
	// REDIS_STREAM_RETENTION). When set, a single goroutine periodically trims
	// the configured stream with XTRIM MINID ~ so entries older than the window
	// are removed. It is completely separate from ACK/retry/DLQ semantics and
	// never touches message processing. A malformed or non-positive value is
	// logged by config.Load and retention is disabled (0) — a typo in this
	// optional variable does not fail startup; unset/empty disables retention
	// entirely.
	if cfg.StreamRetention > 0 {
		go retentionLoop(ctx, client, cfg.RedisStream, cfg.StreamRetention, logger)
	}

	if err := consumer.EnsureGroup(ctx); err != nil {
		logger.Fatalf("Ensure consumer group: %v", err)
	}

	runWorker := runner.NewWithMetrics(prepared, logger, metricsInstance)
	// Stamp the relay.hostname label (the worker/consumer identity) on every
	// execution container. Must be set before Consume begins; it is wired right
	// after construction so all invocations carry it.
	runWorker.SetHostname(cfg.ConsumerName)
	// Wire the local secrets provider. It is infallible to construct (the
	// directory is created lazily on Set, never on Resolve), and a missing
	// secret surfaces as a per-invocation Resolve error rather than a startup
	// failure — startup does not validate that referenced secrets exist.
	secretProvider, err := secrets.NewLocalProvider(secrets.SecretsDir)
	if err != nil {
		logger.Fatalf("Secrets: %v", err)
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
	// them once no in-flight execution uses them. On removal, the RemoveFunction
	// hook first deletes the function's Prometheus series (at the same retirement
	// point, after the registry entry is swapped to nil) and then retires its
	// images; the flush sweep in recordSnapshots below re-deletes any series an
	// in-flight invocation may have recreated after this removal, so the
	// "removed function => no exposed series" invariant holds even mid-invocation.
	rec := reconciler.New(
		reconciler.Config{
			Root:   function.Dir,
			State:  st,
			Retire: func(_ string, oldImage string) { runWorker.RetireImage(oldImage) },
			RemoveFunction: func(name string) {
				metricsInstance.RemoveFunction(name)
				runWorker.RemoveFunctionImages(name)
			},
		},
		runWorker.Registry(),
		manager,
		logger,
	)
	for _, fn := range functions {
		rec.Seed(fn)
	}
	logger.Printf("Watching %s for changes", function.Dir)

	// Runs in its own goroutine and stops when ctx is cancelled.
	go rec.Start(ctx)

	logger.Printf("Consuming stream %q as group %q consumer %q",
		cfg.RedisStream,
		cfg.RedisGroup,
		cfg.ConsumerName,
	)
	if err := consumer.Consume(ctx, runWorker.Handle); err != nil {
		logger.Fatalf("Consume: %v", err)
	}

	// Final flush of the registry into SQLite before the deferred st.Close()
	// runs. Bounded by a short timeout so a wedged SQLite cannot hang shutdown;
	// failure is logged and shutdown continues (telemetry, not state). A no-op
	// when metrics are disabled (nil registry).
	finalStatsFlush(metricsInstance, st)

	// Bounded graceful shutdown of the metrics server, so in-flight scrapes
	// drain rather than being cut off mid-request. This runs after Consume
	// returned on shutdown; the bound comes from a short timeout context so a
	// wedged handler cannot hang shutdown. A no-op when metrics are disabled
	// (the server was never created).
	if metricsServer != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Stop(stopCtx); err != nil {
			logger.Printf("Metrics server: graceful shutdown: %v", err)
		}
	}

	logger.Printf("Shutdown complete")
}

// parkUntilShutdown blocks until ctx is cancelled, then returns. It stands in
// for the stats flush goroutine when metrics are disabled (there is nothing to
// snapshot), keeping shutdown ordering uniform: every background loop the
// worker starts either exits on ctx.Done or is explicitly stopped.
func parkUntilShutdown(ctx context.Context) {
	<-ctx.Done()
}

// restorePersistedStats seeds the fresh process-lifetime metrics registry with
// the cumulative counters persisted in the state database, so the first
// snapshot never resets them. The registry only knows this process's lifetime,
// while the SQLite stats rows are cumulative history; seeding bridges them. It
// is nil-safe on both the registry and the state handle (a nil st means the DB
// failed to open, so there is nothing to restore). Gauges are deliberately NOT
// restored: they are point-in-time backlog snapshots refreshed each snapshot.
func restorePersistedStats(metricsInstance *metrics.Registry, st *state.State) {
	if metricsInstance == nil || st == nil {
		return
	}
	if gs, ok := st.Stats(); ok {
		metricsInstance.SeedCounter("events_processed_total", gs.EventsProcessedTotal)
		metricsInstance.SeedCounter("handler_success_total", gs.HandlerSuccessTotal)
		metricsInstance.SeedCounter("handler_failure_total", gs.HandlerFailureTotal)
		metricsInstance.SeedCounter("retries_total", gs.RetryTotal)
		metricsInstance.SeedCounter("dlq_entries_total", gs.DLQTotal)
	}
	for _, fs := range st.AllFunctionStats() {
		metricsInstance.SeedFunctionStat(metrics.FunctionStat{
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
func snapshotStats(metricsInstance *metrics.Registry) state.Stats {
	if metricsInstance == nil {
		return state.Stats{}
	}
	return state.Stats{
		EventsProcessedTotal:    metricsInstance.Counter("events_processed_total"),
		HandlerSuccessTotal:     metricsInstance.Counter("handler_success_total"),
		HandlerFailureTotal:     metricsInstance.Counter("handler_failure_total"),
		RetryTotal:              metricsInstance.Counter("retries_total"),
		DLQTotal:                metricsInstance.Counter("dlq_entries_total"),
		PendingEntries:          int64(metricsInstance.Gauge("pending_entries")),
		OldestPendingAgeSeconds: int64(metricsInstance.Gauge("pending_oldest_age_seconds")),
	}
}

// funcSnapshotStats maps the registry's per-function counters into the state
// layer's FunctionStats rows. It is nil-safe: a nil registry yields an empty
// slice so the snapshot path can never panic or block processing.
func funcSnapshotStats(metricsInstance *metrics.Registry) []state.FunctionStats {
	if metricsInstance == nil {
		return nil
	}
	stats := metricsInstance.FunctionStatsSnapshot()
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
func statsLoop(
	ctx context.Context,
	metricsInstance *metrics.Registry,
	st *state.State,
	interval time.Duration,
) {
	if st == nil {
		<-ctx.Done()
		return
	}
	recordSnapshots(ctx, st, metricsInstance)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			recordSnapshots(ctx, st, metricsInstance)
		}
	}
}

// finalStatsFlush performs a bounded final flush of the registry into SQLite on
// graceful shutdown, so the last interval of telemetry is not lost. It is
// bounded by a short timeout so a wedged SQLite cannot hang shutdown; on
// timeout or error it logs and returns (telemetry, not state). It is nil-safe
// on both the registry and the state handle.
func finalStatsFlush(metricsInstance *metrics.Registry, st *state.State) {
	if metricsInstance == nil || st == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	recordSnapshots(ctx, st, metricsInstance)
}

// recordSnapshots writes the whole stats snapshot — the global stats row and
// one function_stats row per function with any attributed activity — in a
// single short transaction (see state.RecordStatsSnapshot). Before the write it
// enforces the metrics/SQLite consistency invariant: it sweeps the registry with
// SweepFunctionMetrics against state.FunctionNames, deleting any function-scoped
// series whose function no longer has a functions row. This complements the
// reconciler's RemoveFunction hook (which deletes series at removal time) by
// re-deleting series an in-flight invocation may have recreated after removal —
// a removed function exposes NO series on /metrics, and globals are untouched.
// The transaction first prunes orphaned function_stats rows (a removed
// function's row is not re-created even though its registry counters are swept
// just above). Per-function writes are bounded by the function count, so the 5s
// cadence keeps them small. A failed flush is logged and retried next tick with
// the current absolute values; no path resets counters on failure.
func recordSnapshots(ctx context.Context, st *state.State, metricsInstance *metrics.Registry) {
	// Sweep the registry against the live function set before snapshotting, so
	// a function removed (or swept) this interval cannot persist a stale
	// function_stats row that the orphan-prune would have to reject anyway, and
	// cannot linger on /metrics.
	//
	// Fail-open: if the live set cannot be read (st.FunctionNames returns
	// ok=false), the sweep is skipped entirely rather than run against an empty
	// map — an empty live set would delete every function-scoped series,
	// including live functions'. Sweeping is best-effort and re-applied on the
	// next successful flush; RecordStatsSnapshot below already logs the DB
	// issue, so no new logging is added here.
	if names, ok := st.FunctionNames(); ok {
		active := make(map[string]bool, len(names))
		for _, name := range names {
			active[name] = true
		}
		metricsInstance.SweepFunctionMetrics(active)
	}
	// RecordStatsSnapshot logs internally on error (matching the state package's
	// non-fatal style); the returned error is only for the caller to bound the
	// write with a context.
	_ = st.RecordStatsSnapshot(
		ctx,
		snapshotStats(metricsInstance),
		funcSnapshotStats(metricsInstance),
	)
}

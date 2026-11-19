package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// statsFlushInterval is the fixed PostgreSQL snapshot cadence. Telemetry, not
// event-processing state: Prometheus stays live in-process, while PostgreSQL
// receives the current absolute snapshot every interval. Deliberately NOT
// env/flag-configurable — a fixed cadence keeps the durability model simple
// (a hard crash loses at most one interval of telemetry).
const statsFlushInterval = 5 * time.Second

// ownStatsSnapshotter is the narrow state view the startup stats restore needs.
// *state.State is the production implementation; a unit test can supply a
// lightweight fake returning a fixed atomic snapshot.
type ownStatsSnapshotter interface {
	OwnStatsSnapshot(ctx context.Context) (state.OwnStatsSnapshot, error)
}

// restorePersistedStats seeds the fresh process-lifetime metrics registry with
// THIS worker's cumulative counters persisted in the state database, so the
// first snapshot never resets them. The registry only knows this process's
// lifetime, while the PostgreSQL stats rows are cumulative history; seeding
// bridges them. Only the node's OWN rows (keyed by its NodeID) are read — never
// another node's, so two nodes sharing one database never double-count. It is
// nil-safe on both the registry and the state handle. Gauges are deliberately
// NOT restored: they are point-in-time backlog snapshots refreshed each
// snapshot.
//
// The restore is one atomic state snapshot (data plus reset epochs), and the
// returned generations initialize the stats flusher. A reset that lands after
// this snapshot advances an epoch the first flush detects; a reset before it is
// already reflected in the zeroed data and adopted epochs. Best-effort behavior
// is preserved: a failed snapshot logs in state, seeds nothing, and returns
// unknown epochs so the first flush adopts whatever the database holds.
func restorePersistedStats(
	metricsInstance *metrics.Registry, st ownStatsSnapshotter,
) state.OwnStatsSnapshot {
	unknown := state.OwnStatsSnapshot{Generations: state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	}}
	if st == nil {
		return unknown
	}
	snapshot, err := st.OwnStatsSnapshot(context.Background())
	if err != nil {
		return unknown
	}
	if metricsInstance == nil {
		return snapshot
	}
	gs := snapshot.Stats
	metricsInstance.SeedCounter(metrics.MetricEventsReceived, gs.EventsReceivedTotal)
	metricsInstance.SeedCounter(metrics.MetricEventsMatched, gs.EventsMatchedTotal)
	metricsInstance.SeedCounter(metrics.MetricEventsUnmatched, gs.EventsUnmatchedTotal)
	metricsInstance.SeedCounter(metrics.MetricHandlerSuccess, gs.HandlerSuccessTotal)
	metricsInstance.SeedCounter(metrics.MetricHandlerFailure, gs.HandlerFailureTotal)
	metricsInstance.SeedCounter(metrics.MetricRetries, gs.RetryTotal)
	metricsInstance.SeedCounter(metrics.MetricDLQEntries, gs.DLQTotal)
	for _, fs := range snapshot.AppStats {
		metricsInstance.SeedAppStat(metrics.AppStat{
			App:                 fs.App,
			EventsMatchedTotal:  fs.EventsMatchedTotal,
			HandlerSuccessTotal: fs.HandlerSuccessTotal,
			HandlerFailureTotal: fs.HandlerFailureTotal,
			RetriesTotal:        fs.RetryTotal,
			DLQTotal:            fs.DLQTotal,
			// Restore the cumulative warm-container pool counters so they stay
			// monotonic across restarts (the same contract as the counters
			// above). The live pool gauges are deliberately not restored.
			WarmAcquiresTotal: fs.WarmAcquiresTotal,
			ColdStartsTotal:   fs.ColdStartsTotal,
			DiscardedTotal:    fs.DiscardedTotal,
			// Parse the RFC3339 timestamp strings back to unix seconds for the
			// registry (an unparseable/empty value parses to a zero time, which
			// SeedAppStat skips as "never observed").
			LastExecution: rfc3339ToUnix(fs.LastExecutionAt),
			LastSuccess:   rfc3339ToUnix(fs.LastSuccessAt),
			LastFailure:   rfc3339ToUnix(fs.LastFailureAt),
			LastDLQ:       rfc3339ToUnix(fs.LastDLQAt),
		})
	}
	return snapshot
}

// rfc3339ToUnix parses an RFC3339 timestamp string into its unix-seconds value,
// returning 0 ("never observed") for the empty string or an unparseable value.
// It is the inverse of unixSecToRFC3339 at restore time: the state layer stores
// RFC3339 strings, the registry stores unix seconds, and a stale or corrupt
// persisted value is treated exactly like absence rather than erroring the
// restore path (observability is non-fatal).
func rfc3339ToUnix(s string) int64 {
	if s == "" {
		return 0
	}
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	return parsed.Unix()
}

// unixSecToRFC3339 renders a unix-seconds value as an RFC3339 timestamp,
// rendering 0 ("never observed") as the empty string — the state layer's
// convention for "never". It is the inverse of rfc3339ToUnix at flush time.
func unixSecToRFC3339(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

// relayGlobalCounters are the unlabeled cumulative counters the worker persists.
// They are the global counterpart of the per-app counters read by
// AppStatsSnapshot, and the set the relay baseline captures/subtracts.
var relayGlobalCounters = []string{
	metrics.MetricEventsReceived,
	metrics.MetricEventsMatched,
	metrics.MetricEventsUnmatched,
	metrics.MetricHandlerSuccess,
	metrics.MetricHandlerFailure,
	metrics.MetricRetries,
	metrics.MetricDLQEntries,
}

// relayBaseline is the worker-owned reset baseline captured at the last
// `reset stats`. It is the worker-side replacement for the metrics registry's
// former subtraction baseline: the registry stays a pure, monotonic Prometheus
// accumulator, and the worker subtracts the reset point when it snapshots, so
// the persisted Relay totals continue from zero while /metrics never moves
// backwards. globals maps a counter name to its raw value at the reset; funcs
// maps an app to its raw per-app snapshot at the reset (counters are
// subtracted, and a timestamp not advanced since the reset is suppressed to
// "never observed" — timestamps are last-observed, not cumulative, so
// subtraction alone cannot represent their reset). It is guarded by the owning
// statsFlusher.mu and takes no lock of its own; the zero value is a valid
// "no reset yet" baseline and a nil *relayBaseline is treated the same.
type relayBaseline struct {
	globals map[string]int64
	funcs   map[string]metrics.AppStat
}

// counter returns v (the raw counter value) minus the reset baseline for name.
// A nil or zero baseline returns v unchanged.
func (b *relayBaseline) counter(name string, v int64) int64 {
	if b == nil {
		return v
	}
	return v - b.globals[name]
}

// applyAppStat returns fs with every cumulative per-app counter
// reduced by the reset baseline and any timestamp not advanced since the reset
// suppressed to zero ("never observed"). A nil baseline, or an app absent
// from the baseline, leaves fs unchanged. It is read-only: the registry is
// never mutated.
func (b *relayBaseline) applyAppStat(fs metrics.AppStat) metrics.AppStat {
	if b == nil {
		return fs
	}
	base, ok := b.funcs[fs.App]
	if !ok {
		return fs
	}
	fs.EventsMatchedTotal -= base.EventsMatchedTotal
	fs.HandlerSuccessTotal -= base.HandlerSuccessTotal
	fs.HandlerFailureTotal -= base.HandlerFailureTotal
	fs.RetriesTotal -= base.RetriesTotal
	fs.DLQTotal -= base.DLQTotal
	fs.WarmAcquiresTotal -= base.WarmAcquiresTotal
	fs.ColdStartsTotal -= base.ColdStartsTotal
	fs.DiscardedTotal -= base.DiscardedTotal
	// Timestamps are last-observed, so a value not newer than the reset point
	// belongs to pre-reset history and is reported as "never observed". A
	// post-reset execution always advances the clock past the baseline.
	if fs.LastExecution <= base.LastExecution {
		fs.LastExecution = 0
	}
	if fs.LastSuccess <= base.LastSuccess {
		fs.LastSuccess = 0
	}
	if fs.LastFailure <= base.LastFailure {
		fs.LastFailure = 0
	}
	if fs.LastDLQ <= base.LastDLQ {
		fs.LastDLQ = 0
	}
	return fs
}

// captureRelayBaseline reads the registry's current raw counters and
// per-app snapshot and installs them as the reset baseline. A nil registry
// yields the zero baseline (there is no in-memory source to reset). The caller
// holds the flusher mutex, so the captured values are a consistent reset point
// with respect to any concurrent flush.
func captureRelayBaseline(metricsInstance *metrics.Registry) relayBaseline {
	var b relayBaseline
	if metricsInstance == nil {
		return b
	}
	b.globals = make(map[string]int64, len(relayGlobalCounters))
	for _, name := range relayGlobalCounters {
		b.globals[name] = metricsInstance.Counter(name)
	}
	b.funcs = make(map[string]metrics.AppStat)
	for _, fs := range metricsInstance.AppStatsSnapshot() {
		b.funcs[fs.App] = fs
	}
	return b
}

// snapshotStats maps the metrics registry into the state database's Stats
// payload. The registry counter names feed the persisted Stats fields directly,
// with one rename (retries_total → RetryTotal) and the float gauges truncated to
// int64. Each cumulative counter has the worker-owned relay baseline subtracted
// (base), so an operator reset starts the persisted totals from zero while
// Prometheus stays monotonic; before any reset the baseline is zero and this
// equals the raw counter. The backlog gauges are point-in-time values and are
// never baselined; they are read as ONE observation pair together with their
// observation instant, so a concurrent sampler update cannot persist a mixed
// depth/age pair marked fresh. Until the first complete observation exists, the
// live gauge values are snapshotted without a freshness timestamp. The three
// event-classification counters are copied verbatim, preserving the received ==
// matched + unmatched partition. It is nil-safe: a nil registry yields a zero
// Stats so the snapshot path can never panic or block processing.
func snapshotStats(metricsInstance *metrics.Registry, base *relayBaseline) state.Stats {
	if metricsInstance == nil {
		return state.Stats{}
	}
	pending, oldestAge, pendingAt, _ := metricsInstance.PendingBacklogObservation()
	stats := state.Stats{
		EventsReceivedTotal: base.counter(
			metrics.MetricEventsReceived, metricsInstance.Counter(metrics.MetricEventsReceived)),
		EventsMatchedTotal: base.counter(
			metrics.MetricEventsMatched, metricsInstance.Counter(metrics.MetricEventsMatched)),
		EventsUnmatchedTotal: base.counter(
			metrics.MetricEventsUnmatched, metricsInstance.Counter(metrics.MetricEventsUnmatched)),
		HandlerSuccessTotal: base.counter(
			metrics.MetricHandlerSuccess, metricsInstance.Counter(metrics.MetricHandlerSuccess)),
		HandlerFailureTotal: base.counter(
			metrics.MetricHandlerFailure, metricsInstance.Counter(metrics.MetricHandlerFailure)),
		RetryTotal: base.counter(
			metrics.MetricRetries, metricsInstance.Counter(metrics.MetricRetries)),
		DLQTotal: base.counter(
			metrics.MetricDLQEntries, metricsInstance.Counter(metrics.MetricDLQEntries)),
		PendingEntries:          int64(pending),
		OldestPendingAgeSeconds: int64(oldestAge),
	}
	if !pendingAt.IsZero() {
		stats.PendingObservedAtUnixMs = pendingAt.UnixMilli()
	}
	return stats
}

// snapshotAppStats maps the registry's per-app counters AND latest
// execution-history timestamps into the state layer's AppStats rows. Like
// snapshotStats it applies the worker-owned relay baseline (base), so
// per-app totals continue from zero after an operator reset and pre-reset
// timestamps are reported as "never observed", while the Prometheus series stay
// monotonic. The registry stores unix seconds; the state layer stores RFC3339
// strings in the updated_at convention (empty = never observed), so zero
// timestamps map to "". It is nil-safe: a nil registry yields an empty slice so
// the snapshot path can never panic or block processing.
func snapshotAppStats(metricsInstance *metrics.Registry, base *relayBaseline) []state.AppStats {
	if metricsInstance == nil {
		return nil
	}
	stats := metricsInstance.AppStatsSnapshot()
	out := make([]state.AppStats, 0, len(stats))
	for _, fs := range stats {
		fs = base.applyAppStat(fs)
		out = append(out, state.AppStats{
			App:                 fs.App,
			EventsMatchedTotal:  fs.EventsMatchedTotal,
			HandlerSuccessTotal: fs.HandlerSuccessTotal,
			HandlerFailureTotal: fs.HandlerFailureTotal,
			RetryTotal:          fs.RetriesTotal,
			DLQTotal:            fs.DLQTotal,
			WarmAcquiresTotal:   fs.WarmAcquiresTotal,
			ColdStartsTotal:     fs.ColdStartsTotal,
			DiscardedTotal:      fs.DiscardedTotal,
			LastExecutionAt:     unixSecToRFC3339(fs.LastExecution),
			LastSuccessAt:       unixSecToRFC3339(fs.LastSuccess),
			LastFailureAt:       unixSecToRFC3339(fs.LastFailure),
			LastDLQAt:           unixSecToRFC3339(fs.LastDLQ),
		})
	}
	return out
}

// statsStore is the narrow state view the flusher needs: the live app set for
// the metrics sweep, the fenced snapshot and reset writes, stale-row pruning,
// and the stable node identity used in the ownership-loss log. *state.State is
// the production implementation; a unit test can supply a lightweight fake with
// supplied reset generations so flush and reset logic run without PostgreSQL.
type statsStore interface {
	AppNames(ctx context.Context) ([]string, bool)
	RecordStatsSnapshotWithGenerations(
		ctx context.Context,
		generations state.StatsGenerations,
		stats state.Stats,
		fns []state.AppStats,
	) (state.StatsGenerations, error)
	PruneStaleStats(ctx context.Context, retention time.Duration) (int, error)
	ResetNodeStats(ctx context.Context, nodeID string) (int64, error)
	ResetAllStats(ctx context.Context) (int64, error)
	NodeID() string
}

// statsFlusher owns every write of the registry's Relay-visible totals into
// PostgreSQL and the matching in-memory reset. It is the serialization point that
// makes a socket-triggered `reset stats` deterministic against the periodic
// flush: mu is held across BOTH capturing the snapshot and writing it, and
// ResetStats takes the same mutex, so a flush can never interleave a captured
// pre-reset snapshot with the reset (pre-reset values cannot be resurrected).
// It is safe for concurrent use.
type statsFlusher struct {
	mu sync.Mutex
	st statsStore
	// metrics is the accounting registry the worker always constructs; it is
	// nil only when a caller deliberately passes nil (tests), in which case the
	// snapshot mappers yield zero values. Production always supplies the
	// registry, so a flush never clobbers the persisted cumulative totals.
	metrics *metrics.Registry
	// baseline is the worker-owned reset point captured by ResetStats. It is
	// read and replaced under mu, so a concurrent flush always sees a coherent
	// baseline. Zero value = "no reset yet".
	baseline relayBaseline

	// generation and nodeGeneration are this worker's last-known shared and
	// node-local reset epochs (see state.StatsGenerations). They start at
	// state.StatsGenerationUnknown and are refreshed from the database when the
	// flusher is built, so the first flush adopts the stored epochs instead of a
	// spurious rebase; every flush then tracks the epochs it returns.
	generation     int64
	nodeGeneration int64

	// ownershipLost records that another process incarnation of the same
	// node identity has claimed this worker's stats row, so this (stale) process must
	// stop writing. It is terminal for the process: the instance id is fixed at
	// startup and a stale incarnation can never reclaim the row, so the warning
	// is logged once and no further writes are attempted.
	ownershipLost bool

	// retention bounds stale-owner row growth (see state.PruneStaleStats); <=0
	// disables compaction. lastPrune throttles pruning to one attempt per
	// retentionTickInterval(retention) after a successful flush.
	retention time.Duration
	lastPrune time.Time

	// log records the one-time ownership-loss warning; it defaults to
	// slog.Default() and the worker installs the process logger.
	log *slog.Logger
}

// newStatsFlusher builds a flusher initialized with the epochs from the same
// atomic restore snapshot that seeded the registry. Those epochs are the
// linearization point for the seeded totals: a reset racing startup advances
// one of them and fences the first flush instead of pairing stale seeded totals
// with a newer generation. Callers with no observed epoch pass
// state.StatsGenerationUnknown: an unknown global epoch adopts the stored epoch,
// while an unknown node epoch adopts only a stored zero/absent epoch, so a
// non-zero stored node epoch fences the first flush with ErrStatsResetAdvanced.
// retention and the process logger are installed by the worker after
// construction; tests that only exercise the flush path leave the defaults (no
// compaction, default logger).
func newStatsFlusher(
	st statsStore,
	metricsInstance *metrics.Registry,
	generations state.StatsGenerations,
) *statsFlusher {
	return &statsFlusher{
		st:             st,
		metrics:        metricsInstance,
		generation:     generations.Global,
		nodeGeneration: generations.Node,
		log:            slog.Default(),
	}
}

// flush captures the current Relay-visible snapshot and writes it in one short
// transaction, then opportunistically compacts stale owner rows. Held under mu
// so a concurrent ResetStats cannot land between capture and write. Nil-safe on
// the flusher.
func (flusher *statsFlusher) flush(ctx context.Context) {
	if flusher == nil {
		return
	}
	flusher.mu.Lock()
	defer flusher.mu.Unlock()
	flusher.flushLocked(ctx)
}

// flushLocked does the flush work with mu already held. The write is fenced two
// ways (see state.RecordStatsSnapshot): the epoch must match the flusher's
// known generation, and this process incarnation must still own the stats row.
// A reset that advanced the epoch is handled by rebasing to the registry's
// current values and retrying ONCE, so the reset's zero is not immediately
// re-counted; a lost ownership stops all writes.
func (flusher *statsFlusher) flushLocked(ctx context.Context) {
	// Sweep the registry against the live app set before snapshotting, so an app
	// removed (or swept) this interval cannot persist a stale app_stats row that
	// the orphan-prune would have to reject anyway, and cannot linger on
	// /metrics.
	//
	// Fail-open: if the live set cannot be read (st.AppNames returns ok=false),
	// the sweep is skipped entirely rather than run against an empty map — an
	// empty live set would delete every app-scoped series, including live apps'.
	// Sweeping is best-effort and re-applied on the next successful flush.
	if names, ok := flusher.st.AppNames(ctx); ok {
		active := make(map[string]bool, len(names))
		for _, name := range names {
			active[name] = true
		}
		flusher.metrics.SweepAppMetrics(active)
	}

	// A stale incarnation must not write at all: writing would either fail the
	// ownership predicate or race a reclaim, and any per-app write would be
	// rejected after the global one anyway.
	if flusher.ownershipLost {
		return
	}

	// At most two attempts: the first under the known epochs; if a reset
	// (global, or local to this node) advanced an epoch in the meantime, rebase
	// the in-memory baseline to the registry's current values and retry once
	// under the new epochs. A second advance is left for the next tick rather
	// than looped on.
	for range 2 {
		generations, err := flusher.st.RecordStatsSnapshotWithGenerations(
			ctx,
			state.StatsGenerations{
				Global: flusher.generation,
				Node:   flusher.nodeGeneration,
			},
			snapshotStats(flusher.metrics, &flusher.baseline),
			snapshotAppStats(flusher.metrics, &flusher.baseline),
		)
		switch {
		case err == nil:
			flusher.generation = generations.Global
			flusher.nodeGeneration = generations.Node
			flusher.prune(ctx)
			return
		case errors.Is(err, state.ErrStatsResetAdvanced):
			// The persisted rows were just zeroed by a reset. Rebase so the
			// snapshot restarts from the reset point instead of re-adding the
			// pre-reset totals, and adopt the epochs it happened under.
			flusher.generation = generations.Global
			flusher.nodeGeneration = generations.Node
			flusher.baseline = captureRelayBaseline(flusher.metrics)
		case errors.Is(err, state.ErrStatsOwnershipLost):
			if !flusher.ownershipLost {
				flusher.ownershipLost = true
				flusher.log.Warn(
					"Stats: node ownership lost; another process claimed this node's stats row; stopping stats writes",
					"node_id", flusher.st.NodeID(),
				)
			}
			return
		default:
			// An unexpected failure is already logged by the state package; the
			// next tick retries with the current absolute values.
			return
		}
	}
}

// prune runs best-effort compaction of stale owner rows at most once per
// retentionTickInterval, after a successful flush. retention <=0 disables it.
// The attempted instant is recorded before the call so a persistent prune
// failure does not retry on every tick; a failure is logged and deferred.
func (flusher *statsFlusher) prune(ctx context.Context) {
	if flusher.retention <= 0 {
		return
	}
	now := time.Now()
	if !flusher.lastPrune.IsZero() && now.Sub(flusher.lastPrune) < retentionTickInterval(flusher.retention) {
		return
	}
	flusher.lastPrune = now
	n, err := flusher.st.PruneStaleStats(ctx, flusher.retention)
	if err != nil {
		flusher.log.Warn("Stats: prune stale owners failed; will retry next interval", "error", err)
		return
	}
	if n > 0 {
		flusher.log.Info("Stats: pruned stale owner rows", "removed", n, "retention", flusher.retention)
	}
}

// dropAppBaseline drops an app's entry from the worker-owned reset
// baseline. It is called from the reconciler's RemoveApp hook alongside the
// registry's series deletion, so an app removed and later re-added starts
// from its fresh zero-valued series instead of subtracting a stale pre-removal
// total (which would persist a negative value). It is idempotent, nil-safe, and
// safe to call before any reset (a zero baseline has no entry to drop).
func (flusher *statsFlusher) dropAppBaseline(name string) {
	if flusher == nil {
		return
	}
	flusher.mu.Lock()
	delete(flusher.baseline.funcs, name)
	flusher.mu.Unlock()
}

// NodeID returns the stable node identity this resetter owns, or the empty
// string when no state handle is available. It implements NodeStatsResetter so
// the control socket can verify a local reset's expected NODE_ID before
// touching any counters.
func (flusher *statsFlusher) NodeID() string {
	if flusher == nil || flusher.st == nil {
		return ""
	}
	return flusher.st.NodeID()
}

// ResetStats resets THIS node's accumulated Relay statistics. It captures a
// candidate subtraction baseline BEFORE the state reset and installs it only
// after the reset succeeds, all while holding the SAME mutex flush uses, so no
// captured pre-reset snapshot can be written after the reset. It implements
// StatsResetter for the socket's local `reset stats` command and never mutates
// Prometheus: the registry keeps accumulating monotonically, and the flush
// subtracts the baseline when it snapshots.
//
// Capturing before the reset linearizes the baseline safely: an increment after
// the capture but before the reset commit is zeroed by the reset and excluded
// from the new baseline, so it is not double-counted; an increment after the
// commit is after the linearization point and is therefore counted post-reset.
// Capturing after the commit would drop such increments. A failed reset leaves
// the effective baseline and epochs untouched, while an ambiguous commit outcome
// cannot silently lose or double-count: the persisted node epoch was bumped, so
// the next flush is fenced, rebases to the current registry values, and writes
// them under the new epoch. A missing state handle reports an error so the
// socket answers stats_reset_failed and the CLI surfaces it rather than masking
// a failed worker reset.
func (flusher *statsFlusher) ResetStats() error {
	if flusher == nil {
		return errors.New("stats flusher unavailable")
	}
	flusher.mu.Lock()
	defer flusher.mu.Unlock()
	if flusher.st == nil {
		return errors.New("state database unavailable")
	}
	candidate := captureRelayBaseline(flusher.metrics)
	gen, err := flusher.st.ResetNodeStats(context.Background(), flusher.st.NodeID())
	if err != nil {
		return err
	}
	flusher.baseline = candidate
	flusher.nodeGeneration = gen
	return nil
}

// ResetAllStats resets EVERY node's accumulated Relay statistics through the
// global state reset, installing the candidate baseline and new shared epoch
// only after the reset succeeds, under the same mutex as flush. The candidate is
// captured before the reset so increments committed after the reset's
// linearization point are counted post-reset instead of being lost. It
// implements the socket's `reset stats --all` command; other workers observe the
// advanced shared epoch on their next flush and rebase without losing monotonic
// Prometheus counters.
func (flusher *statsFlusher) ResetAllStats() error {
	if flusher == nil {
		return errors.New("stats flusher unavailable")
	}
	flusher.mu.Lock()
	defer flusher.mu.Unlock()
	if flusher.st == nil {
		return errors.New("state database unavailable")
	}
	candidate := captureRelayBaseline(flusher.metrics)
	gen, err := flusher.st.ResetAllStats(context.Background())
	if err != nil {
		return err
	}
	flusher.baseline = candidate
	flusher.generation = gen
	return nil
}

// statsLoop is the flush loop: it mirrors the in-memory metrics registry into
// the state database on a fixed interval until ctx is cancelled. The first
// flush runs immediately so the stats row exists before the first tick (this
// also makes `relay stats` useful right after startup). It is nil-safe on the
// state handle, so observability can never break processing. The loop's ctx is
// the shutdown ctx; periodic flushes use it directly (a cancelled ctx simply
// stops the loop).
func statsLoop(ctx context.Context, flusher *statsFlusher, interval time.Duration) {
	if flusher == nil || flusher.st == nil {
		<-ctx.Done()
		return
	}
	flusher.flush(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flusher.flush(ctx)
		}
	}
}

// finalStatsFlush performs a final flush of the registry into PostgreSQL on
// graceful shutdown, so the last interval of telemetry is not lost. ctx is the
// shutdown step's fresh, bounded context (the registry derives it from
// context.Background), so a wedged PostgreSQL cannot hang shutdown; on timeout or
// error the flush logs and returns (telemetry, not state). It is nil-safe on
// the flusher. The metrics guard is defensive only for direct test callers that
// deliberately build a flusher over a nil registry: in production the registry
// is ALWAYS constructed (see setupMetrics), independent of METRICS_ADDR, so the
// final flush runs whether or not HTTP exposition is enabled, and a nil
// registry (which would write zero Stats over the persisted totals) never
// occurs.
func finalStatsFlush(ctx context.Context, flusher *statsFlusher) {
	if flusher == nil || flusher.st == nil || flusher.metrics == nil {
		return
	}
	flusher.flush(ctx)
}

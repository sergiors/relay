// Registry and its instrumentation.
//
// This file defines the nil-safe Registry facade over a dedicated Prometheus
// registry, including the counters/histograms/gauges, the instrumented mutation
// and readback methods, the Prometheus exposition Handler, and the Snapshot /
// FunctionStatsSnapshot gather routines. It deliberately holds NO lifecycle
// code and NO routing — the HTTP server (Server), the gauge refresher
// (Refresher), and the periodic snapshot logger (MetricsLogger) each live in
// their own file.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// Label is a single key/value pair attached to a labeled metric. Label sets are
// low-cardinality and bounded by the fixed sets callers pass; they should never
// carry high-cardinality values such as message or event IDs.
type Label struct {
	Name, Value string
}

// buckets are the histogram bucket boundaries. prometheus.DefBuckets is used
// because the observed durations (handler execution and image builds) span the
// sub-second-to-minute range those buckets are designed for, and the project
// has no measured latency SLO to justify a bespoke curve.
var buckets = prometheus.DefBuckets

// Registry is a thin, nil-safe facade over a dedicated Prometheus registry. It
// deliberately does NOT use the global default registry: each Registry owns its
// own prometheus.NewRegistry() so instances never leak collectors to one another
// and tests get an isolated instance per test.
type Registry struct {
	reg *prometheus.Registry

	// Typed handles for quick typed getters and labeled routing.
	counters map[string]prometheus.Counter
	gauges   map[string]prometheus.Gauge

	counterVecs   map[string]*labeledCounterVec
	histogramVecs map[string]*labeledHistogramVec

	// funcTimestampsMu guards funcTimestamps, the per-function latest
	// execution-history timestamps (see SetFunctionTimestamp for the semantics
	// and the deliberate non-Prometheus representation).
	funcTimestampsMu sync.RWMutex

	// funcTimestamps maps a function name to its latest per-kind unix-seconds
	// timestamps. It is the latest-known-value stage of the per-function stats
	// pipeline: the runner overwrites entries as executions happen, and the
	// worker's periodic SQLite snapshot (via FunctionStatsSnapshot) persists
	// whatever it last read. Timestamps are RELAY-side state (when did this
	// function's handler last run?), not scrape-time observations, so they are
	// deliberately NOT Prometheus vecs — a gauge that flips forward on every
	// execution would be misuse, and the worker must read a coherent
	// latest-value struct rather than scrape four series. Removal paths
	// (RemoveFunction / SweepFunctionMetrics) delete entries alongside the
	// function's series so a removed function cannot linger here.
	funcTimestamps map[string][functionTimestampCount]int64
}

// FunctionTimestampKind selects exactly one of the four per-function
// execution-history timestamps (see SetFunctionTimestamp). The constants are
// ordered to also serve as indexes into a function's timestamp array.
type FunctionTimestampKind int

const (
	// FunctionTimestampExecution is the last handler-execution attempt: set
	// when an invocation attempt actually begins (when TryStart claims the
	// invocation on the event path, or when the schedule path begins
	// executing). Retries are executions: every claimed attempt updates it.
	FunctionTimestampExecution FunctionTimestampKind = iota
	// FunctionTimestampSuccess is the last successful handler execution.
	FunctionTimestampSuccess
	// FunctionTimestampFailure is the last failed handler execution attempt
	// (a failed attempt that will still retry counts here — it is not
	// reserved for DLQ-routed failures).
	FunctionTimestampFailure
	// FunctionTimestampDLQ is the last invocation that exhausted its retries
	// and was routed to the DLQ. It is the DLQ attribution point, set only
	// where the runner routes an exhausted invocation to the DLQ — not on
	// every failure.
	FunctionTimestampDLQ

	// functionTimestampCount bounds the kind space (array size below).
	functionTimestampCount
)

// SetFunctionTimestamp records the latest value for exactly one of the four
// kinds on function. The runner always passes time.Now(), and startup seeding
// (SeedFunctionStat) passes the persisted value BEFORE any live execution, so a
// simple overwrite is the correct update rule — there is never a case where a
// caller supplies an older value that must lose to a newer one. Zero values may
// be passed (they stand for "never observed"); seeding deliberately skips them
// before calling, so a persisted zero/empty never materializes as an entry. A
// nil receiver is a no-op; unknown kinds are ignored. It is nil-safe.
func (r *Registry) SetFunctionTimestamp(function string, kind FunctionTimestampKind, ts int64) {
	if r == nil || kind < 0 || kind >= functionTimestampCount {
		return
	}
	r.funcTimestampsMu.Lock()
	defer r.funcTimestampsMu.Unlock()
	if r.funcTimestamps == nil {
		r.funcTimestamps = make(map[string][functionTimestampCount]int64)
	}
	arr := r.funcTimestamps[function]
	arr[kind] = ts
	r.funcTimestamps[function] = arr
}

// deleteFunctionTimestamps drops function's timestamp entry entirely. It is
// shared by RemoveFunction and SweepFunctionMetrics so both retirement paths
// clear the Relay-side timestamps too, not only the Prometheus series. The
// caller must hold funcTimestampsMu.
func (r *Registry) deleteFunctionTimestamps(function string) {
	delete(r.funcTimestamps, function)
}

// labeledCounterVec pairs a CounterVec with the canonical order of its label
// names, so IncLabels can map an incoming []Label onto the fixed label positions
// WithLabelValues expects.
type labeledCounterVec struct {
	order []string
	vec   *prometheus.CounterVec
}

// labeledHistogramVec does the same for HistogramVecs.
type labeledHistogramVec struct {
	order []string
	vec   *prometheus.HistogramVec
}

// New returns an empty Registry backed by a fresh, dedicated Prometheus
// registry registered with the fixed collectors callers use.
func New() *Registry {
	reg := prometheus.NewRegistry()

	r := &Registry{
		reg:           reg,
		counters:      make(map[string]prometheus.Counter, 6),
		gauges:        make(map[string]prometheus.Gauge, 2),
		counterVecs:   make(map[string]*labeledCounterVec, 7),
		histogramVecs: make(map[string]*labeledHistogramVec, 2),
	}

	// Unlabeled counters fed by the runner, stream, and manager (worker reads
	// these via Counter in snapshotStats).
	for _, name := range []string{
		"events_received_total",
		"events_processed_total",
		"retries_total",
		"dlq_entries_total",
		"handler_success_total",
		"handler_failure_total",
		// concurrency_waits_total counts each time an invocation's acquisition
		// of a concurrency slot had to block (regardless of eventual success) —
		// a cheap proxy for slot contention in the runner.
		"concurrency_waits_total",
		// schedule_occurrences_* are the cluster-wide schedule coordination
		// counters (see internal/schedule). They are Prometheus-only: they are
		// deliberately NOT wired into the SQLite stats snapshot.
		"schedule_occurrences_published_total",
		"schedule_occurrences_duplicate_total",
		"schedule_publish_failures_total",
	} {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name})
		reg.MustRegister(c)
		r.counters[name] = c
	}

	// Handler invocation outcomes, broken out by function and handler. The
	// outcome label (success/failure) is the first of the canonical order; the
	// runner passes labels unsorted, so routing matches by name below.
	handlerInvocations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "handler_invocations_total",
	}, []string{"outcome", "function", "handler"})
	reg.MustRegister(handlerInvocations)
	r.counterVecs["handler_invocations_total"] = &labeledCounterVec{
		order: []string{"outcome", "function", "handler"},
		vec:   handlerInvocations,
	}

	// Image build failures, per function. Function names are validated to
	// [a-z0-9][a-z0-9._-]* and bounded by the function count, so this label is
	// low-cardinality.
	buildFailures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "build_failures_total",
	}, []string{"function"})
	reg.MustRegister(buildFailures)
	r.counterVecs["build_failures_total"] = &labeledCounterVec{
		order: []string{"function"},
		vec:   buildFailures,
	}

	// Per-function operational counters, fed by the runner and read by the
	// worker's FunctionStatsSnapshot. Each is keyed by function name only, so
	// the worker can enumerate per-function attribution without scraping the
	// multi-label handler_invocations_total vec. The function label is
	// low-cardinality (bounded by the function count), matching build_failures.
	//
	// Semantics (see runner.Handle): function_events_total counts a function
	// once per event for which at least one of its rules matched — a
	// functions-engaged counter, distinct from the message-level
	// events_processed_total. handler success/failure are per rule execution.
	// function_retries_total counts every failing rule execution that will be
	// retried (a retry driver); function_dlq_total counts a function once when
	// its failing rule execution is the one that exhausts the rule's retry
	// budget (attempt >= 1+retries, per-invocation) and the message is routed
	// to the DLQ.
	for _, name := range []string{
		"function_events_total",
		"function_handler_success_total",
		"function_handler_failure_total",
		"function_retries_total",
		"function_dlq_total",
	} {
		vec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name}, []string{"function"})
		reg.MustRegister(vec)
		r.counterVecs[name] = &labeledCounterVec{order: []string{"function"}, vec: vec}
	}

	// Handler and image-build durations. Both are histograms; the old hand-rolled
	// duration aggregates (count/sum/max) are superseded, so there is no _max
	// series anymore — a histogram's bucket bounds convey the same spread.
	handlerDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "handler_duration_seconds",
		Buckets: buckets,
	}, []string{"function", "handler"})
	reg.MustRegister(handlerDuration)
	r.histogramVecs["handler_duration_seconds"] = &labeledHistogramVec{
		order: []string{"function", "handler"},
		vec:   handlerDuration,
	}

	// Image builds are always labeled by function (see runtime/manager.go); there
	// is deliberately no unlabeled build timer, which would otherwise collide
	// with the labeled histogram under the same name.
	buildDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "function_build_seconds",
		Buckets: buckets,
	}, []string{"function"})
	reg.MustRegister(buildDuration)
	r.histogramVecs["function_build_seconds"] = &labeledHistogramVec{
		order: []string{"function"},
		vec:   buildDuration,
	}

	// Pending-backlog gauges fed by the stream consumer's XPENDING sampler.
	for _, name := range []string{
		"pending_entries",
		"pending_oldest_age_seconds",
	} {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name})
		reg.MustRegister(g)
		r.gauges[name] = g
	}

	// Buffer/backpressure gauges. buffered_events is the stream consumer's
	// current in-flight local buffer occupancy (events read from Redis but not
	// yet finished); in_flight_invocations is the runner's current globally
	// executing invocation count. Both are set on acquire/release.
	for _, name := range []string{
		"buffered_events",
		"in_flight_invocations",
	} {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name})
		reg.MustRegister(g)
		r.gauges[name] = g
	}

	return r
}

// Handler returns the Prometheus exposition handler for this registry —
// nothing more. Routing of paths and methods is the Server's job (see
// server.go), which wraps this handler behind its mux; this method only
// exposes the registry through promhttp.HandlerFor, so a nil receiver returns
// a valid handler serving an empty body and callers never get a nil
// http.Handler (a scrape of a metrics-disabled process never errors).
func (r *Registry) Handler() http.Handler {
	if r == nil {
		return http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
		)
	}
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// Add increments the named counter by n. A nil receiver is a no-op.
func (r *Registry) Add(name string, n int64) {
	if r == nil {
		return
	}
	if c, ok := r.counters[name]; ok {
		c.Add(float64(n))
	}
}

// Inc increments the named counter by one. A nil receiver is a no-op.
func (r *Registry) Inc(name string) {
	r.Add(name, 1)
}

// AddLabels adds n to the labeled counter for name. The label subset is mapped
// onto the metric's canonical label order (values are picked by name, so the
// caller's argument order does not matter). A nil receiver is a no-op; an
// unknown metric name is ignored.
func (r *Registry) AddLabels(name string, labels []Label, n int64) {
	if r == nil {
		return
	}
	if lc, ok := r.counterVecs[name]; ok {
		lc.vec.WithLabelValues(r.values(lc.order, labels)...).Add(float64(n))
	}
}

// IncLabels increments the labeled counter by one. The label subset is mapped
// onto the metric's canonical label order (values are picked by name, so the
// caller's argument order does not matter). A nil receiver is a no-op; an
// unknown metric name is ignored.
func (r *Registry) IncLabels(name string, labels []Label) {
	r.AddLabels(name, labels, 1)
}

// SeedCounter sets the unlabeled counter name to v by adding v to it. It is
// used at worker startup to bridge the process-lifetime registry onto the
// cumulative totals persisted in the state database, so the first snapshot
// never resets them. A nil receiver is a no-op; an unknown name is ignored.
func (r *Registry) SeedCounter(name string, v int64) {
	r.Add(name, v)
}

// SeedFunctionStat restores a function's persisted cumulative counters into the
// per-function CounterVecs, and its persisted execution-history timestamps into
// the timestamp map (zero values are SKIPPED: a persisted zero/empty timestamp
// stands for "never observed" and must not materialize as an entry that could
// later look newer than nothing). It is the labeled counterpart of SeedCounter:
// the worker calls it at startup for every function_stats row so idle functions
// keep their prior totals instead of being reset by the first snapshot. A nil
// receiver is a no-op.
func (r *Registry) SeedFunctionStat(f FunctionStat) {
	if r == nil {
		return
	}
	labels := []Label{{Name: "function", Value: f.Function}}
	r.AddLabels("function_events_total", labels, f.Events)
	r.AddLabels("function_handler_success_total", labels, f.HandlerSuccessTotal)
	r.AddLabels("function_handler_failure_total", labels, f.HandlerFailureTotal)
	r.AddLabels("function_retries_total", labels, f.RetriesTotal)
	r.AddLabels("function_dlq_total", labels, f.DLQTotal)
	r.funcTimestampsMu.Lock()
	if r.funcTimestamps == nil {
		r.funcTimestamps = make(map[string][functionTimestampCount]int64)
	}
	arr := r.funcTimestamps[f.Function]
	if f.LastExecution > 0 {
		arr[FunctionTimestampExecution] = f.LastExecution
	}
	if f.LastSuccess > 0 {
		arr[FunctionTimestampSuccess] = f.LastSuccess
	}
	if f.LastFailure > 0 {
		arr[FunctionTimestampFailure] = f.LastFailure
	}
	if f.LastDLQ > 0 {
		arr[FunctionTimestampDLQ] = f.LastDLQ
	}
	// Store the (possibly untouched) array only when at least one timestamp was
	// seeded — a function with no counters and no timestamps gains nothing.
	if f.LastExecution > 0 || f.LastSuccess > 0 || f.LastFailure > 0 || f.LastDLQ > 0 {
		r.funcTimestamps[f.Function] = arr
	}
	r.funcTimestampsMu.Unlock()
}

// ObserveDuration records a single duration observation against the labeled
// histogram for name (see ObserveDurationLabels). A nil receiver is a no-op.
// The only caller that once used the unlabeled form (function_build_seconds in
// runtime/manager.go) now records the labeled version, so an unlabeled histogram
// is unnecessary and this method routes to the labeled one.
func (r *Registry) ObserveDuration(name string, d time.Duration) {
	r.ObserveDurationLabels(name, nil, d)
}

// ObserveDurationLabels records a single duration observation under a labeled
// key. A nil receiver is a no-op; an unknown metric name is ignored.
func (r *Registry) ObserveDurationLabels(name string, labels []Label, d time.Duration) {
	if r == nil {
		return
	}
	if lh, ok := r.histogramVecs[name]; ok {
		lh.vec.WithLabelValues(r.values(lh.order, labels)...).Observe(d.Seconds())
	}
}

// SetGauge sets the named gauge to v (replacing any previous value). A nil
// receiver is a no-op.
func (r *Registry) SetGauge(name string, v float64) {
	r.SetGaugeLabels(name, nil, v)
}

// SetGaugeLabels sets the labeled gauge to v. There are no labeled gauges in
// the current metric set, so a labeled call is a no-op (only an unlabeled gauge
// with the given name is set). A nil receiver is a no-op.
func (r *Registry) SetGaugeLabels(name string, labels []Label, v float64) {
	if r == nil || len(labels) != 0 {
		return
	}
	if g, ok := r.gauges[name]; ok {
		g.Set(v)
	}
}

// Counter returns the current value of the unlabeled counter with the given
// name, or 0 when it has not been registered. A nil receiver returns 0.
func (r *Registry) Counter(name string) int64 {
	if r == nil {
		return 0
	}
	c, ok := r.counters[name]
	if !ok {
		return 0
	}
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return int64(m.Counter.GetValue())
}

// FunctionStat is the per-function operational snapshot read from the
// function_* CounterVecs. It is the metrics-side view the worker maps into the
// state layer's function_stats table.
type FunctionStat struct {
	Function            string
	Events              int64
	HandlerSuccessTotal int64
	HandlerFailureTotal int64
	RetriesTotal        int64
	DLQTotal            int64

	// Unix-seconds timestamps (0 = never observed), fed by SetFunctionTimestamp
	// and seeded from SQLite at startup (see SeedFunctionStat). They mirror the
	// runner's execution-history attribution:
	//   - LastExecution: the last handler-execution attempt (retries included,
	//     since every claimed attempt is an execution).
	//   - LastSuccess / LastFailure: the last successful / failed handler
	//     execution (a failed attempt that will retry counts as a failure).
	//   - LastDLQ: the last invocation that exhausted its retries and was
	//     routed to the DLQ (the actual DLQ attribution point, NOT every
	//     failure).
	LastExecution int64
	LastSuccess   int64
	LastFailure   int64
	LastDLQ       int64
}

// FunctionStatsSnapshot reads the five per-function CounterVecs and, for every
// function that has at least one series, fills the four execution-history
// timestamp fields from the timestamp map (a function with counters but no
// timestamp entry gets zeros). A timestamp-only entry (in place but no series,
// i.e. all counters zero) does NOT create a snapshot entry on its own: the
// series absence and Prometheus's zero-value lazy semantics make such an entry
// inconsistent with the counters, and the state layer's case-guarded upsert
// preserves persisted timestamps across empty incoming values anyway — so
// dropping a timestamp-only read here never erases SQLite history.
// It is nil-safe and returns nil when no function has been attributed yet. The
// function_* metrics are Relay-specific, so this Relay-specific helper lives
// here rather than in the worker.
func (r *Registry) FunctionStatsSnapshot() []FunctionStat {
	if r == nil {
		return nil
	}
	// Read the timestamp map under its lock FIRST, so a timestamp entry alone
	// (all counters benignly zero) still surfaces in the snapshot, and counters
	// then fill the (possibly zero) timestamp fields for counter-only
	// functions.
	tsByFn := make(map[string][functionTimestampCount]int64)
	r.funcTimestampsMu.RLock()
	for fn, arr := range r.funcTimestamps {
		tsByFn[fn] = arr
	}
	r.funcTimestampsMu.RUnlock()
	// Gather the whole registry once and group the function_* series by function
	// name. The label order is fixed to ["function"], so the single label value
	// is the name.
	families, err := r.reg.Gather()
	if err != nil {
		return nil
	}
	byName := make(map[string]*FunctionStat)
	for _, f := range families {
		name := f.GetName()
		if !isFunctionMetric(name) {
			continue
		}
		for _, m := range f.GetMetric() {
			fn := labelValue(m, "function")
			if fn == "" {
				continue
			}
			fs := byName[fn]
			if fs == nil {
				fs = &FunctionStat{Function: fn}
				byName[fn] = fs
			}
			v := int64(m.Counter.GetValue())
			switch name {
			case "function_events_total":
				fs.Events = v
			case "function_handler_success_total":
				fs.HandlerSuccessTotal = v
			case "function_handler_failure_total":
				fs.HandlerFailureTotal = v
			case "function_retries_total":
				fs.RetriesTotal = v
			case "function_dlq_total":
				fs.DLQTotal = v
			}
		}
	}
	// Merge the timestamp map into the grouped stats: only functions that
	// already surfaced via series get their timestamp fields filled (see the
	// doc comment's rationale for NOT creating timestamp-only entries).
	for fn, arr := range tsByFn {
		if fs, ok := byName[fn]; ok {
			fs.LastExecution = arr[FunctionTimestampExecution]
			fs.LastSuccess = arr[FunctionTimestampSuccess]
			fs.LastFailure = arr[FunctionTimestampFailure]
			fs.LastDLQ = arr[FunctionTimestampDLQ]
		}
	}
	if len(byName) == 0 {
		return nil
	}
	out := make([]FunctionStat, 0, len(byName))
	for _, fs := range byName {
		out = append(out, *fs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Function < out[j].Function })
	return out
}

// isFunctionMetric reports whether name is one of the per-function CounterVecs
// read by FunctionStatsSnapshot.
func isFunctionMetric(name string) bool {
	switch name {
	case "function_events_total",
		"function_handler_success_total",
		"function_handler_failure_total",
		"function_retries_total",
		"function_dlq_total":
		return true
	}
	return false
}

// functionMetrics lists the labeled vecs that carry the function label, in
// registration order. Function lifecycle cleanup iterates it so a removed
// function's series are deleted across every function-scoped collector in one
// pass. Global (unlabeled) metrics are deliberately absent: they are
// process-lifetime and never deleted.
var functionMetrics = []string{
	"handler_invocations_total",
	"build_failures_total",
	"function_events_total",
	"function_handler_success_total",
	"function_handler_failure_total",
	"function_retries_total",
	"function_dlq_total",
	"handler_duration_seconds",
	"function_build_seconds",
}

// isFunctionCarryingMetric reports whether name is one of the labeled vecs that
// carry the function label (see functionMetrics). It is the sweep's filter: a
// family is only swept when its series are function-scoped, so global metrics
// are never touched.
func isFunctionCarryingMetric(name string) bool {
	for _, n := range functionMetrics {
		if n == name {
			return true
		}
	}
	return false
}

// RemoveFunction deletes every metric series labeled function=<name> across all
// function-scoped vecs. Global metrics and other functions' series are
// untouched. Nil-safe; unknown names are a no-op. It is the reconciliation-time
// cleanup: when a function directory vanishes, the wired RemoveFunction hook
// (see internal/worker) calls this so its stale Prometheus series do not linger
// on /metrics after SQLite state is dropped.
//
// handler_invocations_total and handler_duration_seconds carry a second
// variable label alongside function (outcome/handler respectively), and
// prometheus DeleteLabelValues requires a value for EVERY variable label — so
// those two are deleted by partial match on the function label. Every
// single-function-label vec is deleted by label value. DeletePartialMatch and
// DeleteLabelValues each lock the vec's metricMap internally, so no additional
// lock is taken here. It is idempotent: calling it repeatedly (or for a name
// with no series) is safe and a no-op. The Relay-side timestamp entry is deleted
// alongside the series, so a removed function never lingers in
// FunctionStatsSnapshot via its timestamps.
func (r *Registry) RemoveFunction(name string) {
	if r == nil {
		return
	}
	for _, n := range functionMetrics {
		r.deleteFunction(n, name)
	}
	r.funcTimestampsMu.Lock()
	r.deleteFunctionTimestamps(name)
	r.funcTimestampsMu.Unlock()
}

// SweepFunctionMetrics deletes stale function-scoped series for every function
// name NOT in live. It is used by the worker's stats flush to enforce the
// "removed function => no exposed series" invariant even when an in-flight
// invocation recreates a series after RemoveFunction: the live set comes from
// the state database (state.FunctionNames), and any series whose function is not
// live is swept. Global metrics and live functions' series are untouched.
// Nil-safe; a Gather error returns silently (metrics are best-effort).
func (r *Registry) SweepFunctionMetrics(live map[string]bool) {
	if r == nil {
		return
	}
	families, err := r.reg.Gather()
	if err != nil {
		return
	}
	for _, f := range families {
		name := f.GetName()
		if !isFunctionCarryingMetric(name) {
			continue
		}
		// Collect the function label values present in this family, then delete
		// each that is not live. Deleting inside the same pass is fine: gathered
		// metrics are a snapshot, and the delete APIs lock the vec internally.
		for _, m := range f.GetMetric() {
			fn := labelValue(m, "function")
			if fn == "" || live[fn] {
				continue
			}
			r.deleteFunction(name, fn)
		}
	}
	// Sweep the timestamp map with the same live set, so a removed function's
	// Relay-side execution-history entry is dropped alongside its series.
	r.funcTimestampsMu.Lock()
	for fn := range r.funcTimestamps {
		if !live[fn] {
			delete(r.funcTimestamps, fn)
		}
	}
	r.funcTimestampsMu.Unlock()
}

// deleteFunction removes every series labeled function=fn from the named vec,
// using the delete strategy appropriate to its label set. It is shared by
// RemoveFunction and SweepFunctionMetrics so both retirement paths behave
// identically. Each vec that carries function plus another variable label — the
// counter handler_invocations_total (outcome,function,handler) and the
// histogram handler_duration_seconds (function,handler) — is deleted by
// partial match: prometheus DeleteLabelValues requires a value for EVERY
// variable label, so passing only the function name matches nothing on a
// multi-label vec. Every single-function-label vec is deleted by label value
// directly.
//
// WARNING: any NEW vec carrying the function label must be added to
// functionMetrics AND, when it has more than the single function variable
// label, classified for DeletePartialMatch by setting the `partial` flag below —
// otherwise function lifecycle cleanup silently misses it.
func (r *Registry) deleteFunction(name, fn string) {
	partial := name == "handler_invocations_total" || name == "handler_duration_seconds"
	if lc, ok := r.counterVecs[name]; ok {
		if partial {
			lc.vec.DeletePartialMatch(prometheus.Labels{"function": fn})
		} else {
			lc.vec.DeleteLabelValues(fn)
		}
		return
	}
	if lh, ok := r.histogramVecs[name]; ok {
		if partial {
			lh.vec.DeletePartialMatch(prometheus.Labels{"function": fn})
		} else {
			// function_build_seconds (sole label).
			lh.vec.DeleteLabelValues(fn)
		}
	}
}

// labelValue returns the value of the named label on a gathered metric, or ""
// when absent.
func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// Gauge returns the current value of the unlabeled gauge with the given name,
// or 0 when it has not been registered. A nil receiver returns 0.
func (r *Registry) Gauge(name string) float64 {
	if r == nil {
		return 0
	}
	g, ok := r.gauges[name]
	if !ok {
		return 0
	}
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		return 0
	}
	return m.Gauge.GetValue()
}

// Snapshot renders every registered metric as a logfmt-style line, one per
// metric, sorted for deterministic output:
//
//	name count=N
//	name{a=1,b=2} value=V
//	name{a=1,b=2} count=N sum=1.230
//
// Counters render as `count=N`, gauges as `value=V`, and histograms as
// `count=N sum=X.XXX` (seconds). It returns "" when empty or when r is nil.
func (r *Registry) Snapshot() string {
	if r == nil {
		return ""
	}
	families, err := r.reg.Gather()
	if err != nil {
		return ""
	}
	var lines []string
	for _, f := range families {
		switch f.GetType() {
		case dto.MetricType_COUNTER:
			for _, m := range f.GetMetric() {
				if m.Counter.GetValue() == 0 {
					continue
				}
				lines = append(lines, fmt.Sprintf("%s count=%d", sampleName(f.GetName(), m), int64(m.Counter.GetValue())))
			}
		case dto.MetricType_GAUGE:
			for _, m := range f.GetMetric() {
				lines = append(lines, fmt.Sprintf("%s value=%g", sampleName(f.GetName(), m), m.Gauge.GetValue()))
			}
		case dto.MetricType_HISTOGRAM:
			for _, m := range f.GetMetric() {
				h := m.GetHistogram()
				if h.GetSampleCount() == 0 {
					continue
				}
				lines = append(lines, fmt.Sprintf("%s count=%d sum=%.3f", sampleName(f.GetName(), m), int64(h.GetSampleCount()), h.GetSampleSum()))
			}
		}
	}
	if len(lines) == 0 {
		return ""
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// sampleName renders a single gathered metric as "name" or "name{a=1,b=2}" in
// the logfmt convention, using the family's label set (sorted by name).
func sampleName(familyName string, m *dto.Metric) string {
	if len(m.GetLabel()) == 0 {
		return familyName
	}
	parts := make([]string, 0, len(m.GetLabel()))
	for _, l := range m.GetLabel() {
		parts = append(parts, fmt.Sprintf("%s=%s", l.GetName(), l.GetValue()))
	}
	sort.Strings(parts)
	return familyName + "{" + strings.Join(parts, ",") + "}"
}

// values maps a []Label onto the labeled collector's canonical label positions
// by name. Labels for unknown positions are ignored; a duplicate label name
// collapses into the first matching slot (last write wins within the loop, but
// callers always pass distinct names).
func (r *Registry) values(order []string, labels []Label) []string {
	vals := make([]string, len(order))
	for _, l := range labels {
		for i, n := range order {
			if n == l.Name {
				vals[i] = l.Value
			}
		}
	}
	return vals
}

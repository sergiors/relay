package metrics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	// function_retries_total counts every failing rule execution (a retry
	// driver); function_dlq_total counts a function once when its failing rule
	// execution is the one that exhausts the delivery attempts (attempt >=
	// stream.DefaultMaxAttempts) and the message is routed to the DLQ.
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

	return r
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

// IncLabels increments the labeled counter by one. The label subset is mapped
// onto the metric's canonical label order (values are picked by name, so the
// caller's argument order does not matter). A nil receiver is a no-op; an
// unknown metric name is ignored.
func (r *Registry) IncLabels(name string, labels []Label) {
	if r == nil {
		return
	}
	if lc, ok := r.counterVecs[name]; ok {
		lc.vec.WithLabelValues(r.values(lc.order, labels)...).Inc()
	}
}

// ObserveDuration records a single duration observation against the labeled
// histogram for name (see ObserveDurationLabels). A nil receiver is a no-op;
// the only caller that once used the unlabeled form (function_build_seconds in
// runtime/manager.go) now records the labeled version, so an unlabeled
// histogram is unnecessary and this method routes to the labeled one.
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

// SetGauge sets the named gauge to v (replacing any previous value).
// A nil receiver is a no-op.
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
}

// FunctionStatsSnapshot reads the five per-function CounterVecs and groups them
// by function name, returning one FunctionStat per function that has any
// non-zero counter. It is nil-safe and returns nil when no function has been
// attributed yet. The function_* metrics are Relay-specific, so this
// Relay-specific helper lives here rather than in the worker.
func (r *Registry) FunctionStatsSnapshot() []FunctionStat {
	if r == nil {
		return nil
	}
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

// LogLoop exposes the registry on a fixed interval until ctx is cancelled. Each
// tick, if Snapshot() is non-empty, every metric line is logged (prefixed with
// "metrics"). It is intended to run from `go` in the worker; the stream package
// also logs snapshots from its own sampler. A nil *Registry simply returns, so
// this is safe to launch even when metrics are disabled.
func (r *Registry) LogLoop(ctx context.Context, interval time.Duration, logf func(format string, args ...any)) {
	if r == nil {
		<-ctx.Done()
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := r.Snapshot()
			if s == "" {
				continue
			}
			for _, line := range strings.Split(s, "\n") {
				logf("metrics %s", line)
			}
		}
	}
}

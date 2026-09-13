// Periodic log emission of the registry snapshot.
//
// The MetricsLogger owns the fixed-interval ticker that logs the registry's
// snapshot as logfmt lines, decoupling "collection/state" (Registry) from
// "periodic logging" (MetricsLogger). It reads the SAME registry the HTTP server
// serves on /metrics: both expose the single in-memory source of truth, so a
// logged snapshot and a concurrent scrape never disagree about the current
// values.
package metrics

import (
	"context"
	"strings"
	"time"
)

// DefaultLogInterval is the fixed interval at which the worker logs a metrics
// snapshot. It is intentionally not configurable.
const DefaultLogInterval = 30 * time.Second

// MetricsLogger periodically logs the registry snapshot. It owns the ticker;
// each tick, if Snapshot() is non-empty, every metric line is logged prefixed
// with "metrics". It reads from the same *Registry the Server serves on
// /metrics. It is intended to run from `go` in the worker; the stream consumer
// samples its own pending gauges via a Refresher elsewhere.
type MetricsLogger struct {
	reg      *Registry
	interval time.Duration
	logf     func(string, ...any)
}

// NewMetricsLogger builds a MetricsLogger that logs reg's snapshot every
// interval. A nil reg is safe: Start blocks until ctx is done and never logs. A
// nil logf is also safe (ticks are consumed with nothing emitted), mirroring
// Server's nil-tolerant logf.
func NewMetricsLogger(reg *Registry, interval time.Duration, logf func(string, ...any)) *MetricsLogger {
	if interval <= 0 {
		interval = DefaultLogInterval
	}
	return &MetricsLogger{reg: reg, interval: interval, logf: logf}
}

// Start logs the registry snapshot on the configured interval until ctx is
// cancelled. A nil registry simply blocks until ctx.Done() then returns, so this
// is safe to launch even when metrics are disabled.
func (ml *MetricsLogger) Start(ctx context.Context) {
	if ml.reg == nil {
		<-ctx.Done()
		return
	}
	t := time.NewTicker(ml.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := ml.reg.Snapshot()
			if s == "" {
				continue
			}
			for _, line := range strings.Split(s, "\n") {
				if ml.logf != nil {
					ml.logf("metrics %s", line)
				}
			}
		}
	}
}

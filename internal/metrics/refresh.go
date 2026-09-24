// Periodic refresh of registered gauges.
//
// The Refresher owns the fixed-interval ticker that drives external-lookup
// gauge sources (e.g. Redis XPENDING depth), decoupling "collection/state"
// (Registry + GaugeSource) from "periodic refresh" (Refresher). It is the gauge
// counterpart of MetricsLogger: that reads the in-memory snapshot, this updates
// point-in-time backdrop gauges from external systems.
package metrics

import (
	"context"
	"time"
)

// GaugeSource refreshes one or more gauges by performing external lookups and
// recording their results via SetGauge. Implementations must not panic on error;
// they should log-and-skip, mirroring the stream consumer's XPENDING sampler.
// Refresh is passed the Refresher's ctx so external lookups can be
// context-bounded.
type GaugeSource interface {
	// Refresh performs an external lookup and records the resulting gauge
	// values. It must be safe for the Refresher to call once per tick.
	Refresh(ctx context.Context)
}

// Refresher periodically invokes each GaugeSource's Refresh, passing its own
// ctx so lookups can be bounded. It is intended to run from `go`; each tick
// every source's Refresh runs (sequentially, in registration order) and the
// loop exits when ctx is cancelled. A nil source in the slice is skipped.
type Refresher struct {
	sources  []GaugeSource
	interval time.Duration
}

// NewRefresher builds a Refresher that calls each source's Refresh every
// interval.
func NewRefresher(interval time.Duration, sources ...GaugeSource) *Refresher {
	return &Refresher{sources: sources, interval: interval}
}

// Start runs each source's Refresh on the configured interval until ctx is
// cancelled. Sources are invoked sequentially, in registration order.
func (ref *Refresher) Start(ctx context.Context) {
	ticker := time.NewTicker(ref.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ref.refreshAll(ctx)
		}
	}
}

// refreshAll invokes every non-nil source's Refresh with ctx, in order.
func (ref *Refresher) refreshAll(ctx context.Context) {
	for _, src := range ref.sources {
		if src == nil {
			continue
		}
		src.Refresh(ctx)
	}
}

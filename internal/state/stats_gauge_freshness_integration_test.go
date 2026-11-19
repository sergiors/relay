//go:build integration

package state

import (
	"context"
	"testing"
)

// This file pins the cross-node backlog gauge contract at the READ boundary the
// CLI uses: the aggregate presents exactly ONE complete observed pair, chosen
// by its successful-observation instant; a legacy payload without an
// observation instant never supplies aggregate gauges (its row updated_at
// cannot promote it); and a node-specific read shows that row's own gauges
// verbatim, legacy or not.

// TestAggregateGaugesUseObservationFreshnessOnly walks one backlog's history
// across two nodes: an unobserved (legacy) node, a node that observed the
// backlog, a newer legacy write, and finally a newer observation — the
// aggregate's gauge pair must follow the freshest COMPLETE observation only.
func TestAggregateGaugesUseObservationFreshnessOnly(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	legacy := openWithIdentity(t, dsn, uniqueNodeID(t, "gauge-legacy"), "inst-1")
	observed := openWithIdentity(t, dsn, uniqueNodeID(t, "gauge-observed"), "inst-2")

	// A legacy payload: gauges but no successful-observation instant.
	legacy.RecordStats(Stats{
		EventsMatchedTotal:      7,
		PendingEntries:          500,
		OldestPendingAgeSeconds: 900,
	})

	// The node-specific read keeps the legacy row's own gauges verbatim —
	// that row really did report them at some point.
	own, ok, err := legacy.ReadNodeStats(ctx, legacy.NodeID())
	if err != nil || !ok {
		t.Fatalf("node read: ok=%v err=%v", ok, err)
	}
	if own.PendingEntries != 500 || own.OldestPendingAgeSeconds != 900 {
		t.Fatalf("node gauges = %+v, want the legacy (500, 900) pair", own)
	}

	// The aggregate must NOT source gauges from a row that never observed the
	// backlog — even though it is currently the only row.
	agg, ok, err := legacy.ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("aggregate read: ok=%v err=%v", ok, err)
	}
	if agg.EventsMatchedTotal != 7 {
		t.Fatalf("aggregate events = %d, want 7 (counters always sum)", agg.EventsMatchedTotal)
	}
	if agg.PendingEntries != 0 || agg.OldestPendingAgeSeconds != 0 ||
		agg.PendingObservedAtUnixMs != 0 {
		t.Fatalf("aggregate surfaced legacy gauges: %+v", agg)
	}

	// An observed pair supplies the aggregate gauges as ONE unit.
	observed.RecordStats(Stats{
		EventsMatchedTotal:      3,
		PendingEntries:          4,
		OldestPendingAgeSeconds: 5,
		PendingObservedAtUnixMs: 100,
	})
	agg, ok, err = legacy.ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("aggregate read: ok=%v err=%v", ok, err)
	}
	if agg.EventsMatchedTotal != 10 {
		t.Fatalf("aggregate events = %d, want 10", agg.EventsMatchedTotal)
	}
	if agg.PendingEntries != 4 || agg.OldestPendingAgeSeconds != 5 ||
		agg.PendingObservedAtUnixMs != 100 {
		t.Fatalf("aggregate gauges = %+v, want the observed (4, 5) pair at 100", agg)
	}

	// A NEWER legacy write — bigger gauges, newest row updated_at — still
	// loses: freshness comes from the observation instant, and an unobserved
	// row is never fresher than an observed one.
	legacy.RecordStats(Stats{
		EventsMatchedTotal:      9,
		PendingEntries:          777,
		OldestPendingAgeSeconds: 888,
	})
	agg, _, err = legacy.ReadStats(ctx)
	if err != nil {
		t.Fatalf("aggregate read: %v", err)
	}
	if agg.PendingEntries != 4 || agg.OldestPendingAgeSeconds != 5 ||
		agg.PendingObservedAtUnixMs != 100 {
		t.Fatalf("newer legacy write displaced the observed pair: %+v", agg)
	}
	if agg.EventsMatchedTotal != 12 {
		t.Fatalf("aggregate events = %d, want 12 (9 + 3, both counters still sum)", agg.EventsMatchedTotal)
	}

	// Once the legacy node observes the backlog LATER than the other node, its
	// complete pair takes over the aggregate as a unit.
	legacy.RecordStats(Stats{
		EventsMatchedTotal:      9,
		PendingEntries:          2,
		OldestPendingAgeSeconds: 3,
		PendingObservedAtUnixMs: 200,
	})
	agg, _, err = legacy.ReadStats(ctx)
	if err != nil {
		t.Fatalf("aggregate read: %v", err)
	}
	if agg.PendingEntries != 2 || agg.OldestPendingAgeSeconds != 3 ||
		agg.PendingObservedAtUnixMs != 200 {
		t.Fatalf("aggregate gauges = %+v, want the freshest observed (2, 3) pair at 200", agg)
	}

	// The other node's own read is unaffected by the aggregate's choice.
	other, ok, err := observed.ReadNodeStats(ctx, observed.NodeID())
	if err != nil || !ok {
		t.Fatalf("observed node read: ok=%v err=%v", ok, err)
	}
	if other.PendingEntries != 4 || other.OldestPendingAgeSeconds != 5 {
		t.Fatalf("observed node gauges = %+v, want its own (4, 5) pair", other)
	}
}

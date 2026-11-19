package state

import "testing"

// TestMergeStatsEqualObservationTieIsDeterministic pins the equal-observation
// tie break in mergeStats: two snapshots of the SAME shared backlog sampled at
// the SAME successful-observation instant cannot be ordered by freshness, so
// the aggregate takes the larger complete pair (depth, then age). Folding the
// two rows in either order therefore yields the identical result, so the
// aggregate never depends on the order the stats rows happen to be read in.
func TestMergeStatsEqualObservationTieIsDeterministic(t *testing.T) {
	const ts = "2026-01-01T00:00:00Z"
	const observed = int64(1767225600000)
	low := Stats{
		UpdatedAt:               ts,
		PendingEntries:          2,
		OldestPendingAgeSeconds: 3,
		PendingObservedAtUnixMs: observed,
	}
	high := Stats{
		UpdatedAt:               ts,
		PendingEntries:          9,
		OldestPendingAgeSeconds: 5,
		PendingObservedAtUnixMs: observed,
	}

	forward := low
	mergeStats(&forward, high)
	reverse := high
	mergeStats(&reverse, low)

	if forward != reverse {
		t.Fatalf("tie merge is order-dependent: forward=%+v reverse=%+v", forward, reverse)
	}
	if forward.PendingEntries != 9 || forward.OldestPendingAgeSeconds != 5 {
		t.Fatalf("tie gauges = (%d, %d), want the larger (9, 5)",
			forward.PendingEntries, forward.OldestPendingAgeSeconds)
	}
	if forward.PendingObservedAtUnixMs != observed {
		t.Fatalf("tie observation instant = %d, want %d", forward.PendingObservedAtUnixMs, observed)
	}
	if forward.UpdatedAt != ts {
		t.Fatalf("tie updated_at = %q, want %q", forward.UpdatedAt, ts)
	}
}

// TestMergeStatsFreshestObservationOwnsGauges pins the freshness rule: the
// aggregate's backlog pair comes from the row with the newest
// successful-observation instant — never from row updated_at, which also
// advances when a backlog sample failed — and the whole pair moves together.
func TestMergeStatsFreshestObservationOwnsGauges(t *testing.T) {
	stale := Stats{
		UpdatedAt:               "2026-01-01T00:00:00Z",
		PendingEntries:          500,
		OldestPendingAgeSeconds: 900,
		PendingObservedAtUnixMs: 1000,
	}
	fresh := Stats{
		UpdatedAt:               "2025-12-31T23:59:00Z",
		EventsMatchedTotal:      4,
		PendingEntries:          2,
		OldestPendingAgeSeconds: 3,
		PendingObservedAtUnixMs: 2000,
	}

	// Read in either order the freshest observation's complete pair wins, even
	// though the stale row carries the newer row write time and larger gauges.
	for _, order := range [][2]Stats{{stale, fresh}, {fresh, stale}} {
		var out Stats
		mergeStats(&out, order[0])
		mergeStats(&out, order[1])
		if out.PendingEntries != 2 || out.OldestPendingAgeSeconds != 3 ||
			out.PendingObservedAtUnixMs != 2000 {
			t.Fatalf("merge(%+v, %+v) gauges = (%d, %d, %d), want the freshest pair (2, 3, 2000)",
				order[0], order[1],
				out.PendingEntries, out.OldestPendingAgeSeconds, out.PendingObservedAtUnixMs)
		}
		if out.EventsMatchedTotal != 4 {
			t.Fatalf("counters must still sum across rows, got %d", out.EventsMatchedTotal)
		}
		if out.UpdatedAt != "2026-01-01T00:00:00Z" {
			t.Fatalf("updated_at = %q, want the newest row write time", out.UpdatedAt)
		}
	}
}

// TestMergeStatsLegacyRowNeverSuppliesGauges pins the legacy contract: a row
// without a successful-observation instant (a payload written before backlog
// observations existed, or by a caller that never observed the backlog) never
// contributes gauges to the cross-node aggregate — it neither seeds a zero
// aggregate nor displaces an observed pair — while its counters still sum.
func TestMergeStatsLegacyRowNeverSuppliesGauges(t *testing.T) {
	const ts = "2026-01-01T00:00:00Z"
	legacy := Stats{
		UpdatedAt:               ts,
		EventsMatchedTotal:      7,
		PendingEntries:          500,
		OldestPendingAgeSeconds: 900,
	}
	observed := Stats{
		UpdatedAt:               ts,
		PendingEntries:          2,
		OldestPendingAgeSeconds: 3,
		PendingObservedAtUnixMs: 10,
	}

	var out Stats
	mergeStats(&out, legacy)
	if out.PendingEntries != 0 || out.OldestPendingAgeSeconds != 0 ||
		out.PendingObservedAtUnixMs != 0 {
		t.Fatalf("legacy row seeded aggregate gauges: %+v", out)
	}
	if out.EventsMatchedTotal != 7 {
		t.Fatalf("legacy counters = %d, want 7 (counters always sum)", out.EventsMatchedTotal)
	}

	mergeStats(&out, observed)
	if out.PendingEntries != 2 || out.OldestPendingAgeSeconds != 3 ||
		out.PendingObservedAtUnixMs != 10 {
		t.Fatalf("observed pair not selected: %+v", out)
	}

	// A later legacy row with larger gauges must not displace the observed pair.
	laterLegacy := legacy
	laterLegacy.EventsMatchedTotal = 1
	mergeStats(&out, laterLegacy)
	if out.PendingEntries != 2 || out.OldestPendingAgeSeconds != 3 ||
		out.PendingObservedAtUnixMs != 10 {
		t.Fatalf("legacy row displaced the observed pair: %+v", out)
	}
	if out.EventsMatchedTotal != 8 {
		t.Fatalf("counters after later legacy row = %d, want 8", out.EventsMatchedTotal)
	}
}

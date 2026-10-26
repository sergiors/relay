package state

import "testing"

// TestMergeStatsEqualUpdatedAtTieIsDeterministic pins the equal-updated_at tie
// break in mergeStats directly: two snapshots of the SAME shared backlog with the
// same updated_at (RFC3339 has second granularity) cannot be ordered by
// freshness, so each gauge takes the larger value. Folding the two rows in either
// order therefore yields the identical result, so the aggregate never depends on
// the order the stats rows happen to be read in.
func TestMergeStatsEqualUpdatedAtTieIsDeterministic(t *testing.T) {
	const ts = "2026-01-01T00:00:00Z"
	low := Stats{UpdatedAt: ts, PendingEntries: 2, OldestPendingAgeSeconds: 3}
	high := Stats{UpdatedAt: ts, PendingEntries: 9, OldestPendingAgeSeconds: 5}

	forward := low
	mergeStats(&forward, high)
	reverse := high
	mergeStats(&reverse, low)

	if forward != reverse {
		t.Fatalf("tie merge is order-dependent: forward=%+v reverse=%+v", forward, reverse)
	}
	if forward.PendingEntries != 9 || forward.OldestPendingAgeSeconds != 5 {
		t.Fatalf("tie gauges = (%d, %d), want the larger (9, 5)", forward.PendingEntries, forward.OldestPendingAgeSeconds)
	}
	if forward.UpdatedAt != ts {
		t.Fatalf("tie updated_at = %q, want %q", forward.UpdatedAt, ts)
	}
}

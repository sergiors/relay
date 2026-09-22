package state

import (
	"context"
	"testing"
)

// TestFunctionStatsPoolCountersRoundTrip verifies RecordFunctionStats persists
// the three cumulative warm-container pool counters and FunctionStats reads them
// back, including the absent-row zero value.
func TestFunctionStatsPoolCountersRoundTrip(t *testing.T) {
	c := openTestState(t)
	in := FunctionStats{
		Function:          "alpha",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	}
	c.RecordFunctionStats(in)

	s, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected function stats row after record")
	}
	in.UpdatedAt = s.UpdatedAt
	if s != in {
		t.Fatalf("function stats = %+v, want %+v", s, in)
	}

	// A repeated record REPLACES the pool counters (absolute snapshot).
	c.RecordFunctionStats(FunctionStats{Function: "alpha", WarmAcquiresTotal: 9, ColdStartsTotal: 4, DiscardedTotal: 5})
	s, _ = c.FunctionStats("alpha")
	if s.WarmAcquiresTotal != 9 || s.ColdStartsTotal != 4 || s.DiscardedTotal != 5 {
		t.Fatalf("replaced pool counters = %+v", s)
	}

	// An absent function reads zero pool counters.
	if z, ok := c.FunctionStats("ghost"); ok || z != (FunctionStats{}) {
		t.Fatalf("absent function = %+v, ok=%v; want zero,false", z, ok)
	}
}

// TestRecordStatsSnapshotPersistsPoolCounters verifies the worker flush path
// persists pool counters alongside the operational counters and idempotently
// replaces them, and that AllFunctionStats reads them back.
func TestRecordStatsSnapshotPersistsPoolCounters(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	fns := []FunctionStats{
		{Function: "alpha", EventsMatchedTotal: 10, WarmAcquiresTotal: 7, ColdStartsTotal: 3, DiscardedTotal: 2},
	}
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsMatchedTotal: 10}, fns); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s, ok := c.FunctionStats("alpha")
	if !ok || s.WarmAcquiresTotal != 7 || s.ColdStartsTotal != 3 || s.DiscardedTotal != 2 {
		t.Fatalf("alpha after snapshot = %+v, ok=%v", s, ok)
	}

	// A repeat flush with identical values is idempotent (absolute, not delta).
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsMatchedTotal: 10}, fns); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	s, _ = c.FunctionStats("alpha")
	if s.WarmAcquiresTotal != 7 || s.ColdStartsTotal != 3 || s.DiscardedTotal != 2 {
		t.Fatalf("pool counters double-counted: %+v", s)
	}

	all := c.AllFunctionStats()
	if len(all) != 1 || all[0].WarmAcquiresTotal != 7 || all[0].ColdStartsTotal != 3 || all[0].DiscardedTotal != 2 {
		t.Fatalf("AllFunctionStats = %+v", all)
	}
}

package state

import (
	"context"
	"testing"
	"time"
)

// TestAppStatsTimestampsRoundTrip verifies RecordAppStatsContext
// persists the four execution-history timestamp columns and AppStats
// reads them back as RFC3339 strings with UpdatedAt round-trip semantics.
func TestAppStatsTimestampsRoundTrip(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-2 * time.Minute).UTC()
	in := AppStats{
		App:                 "alpha",
		EventsMatchedTotal:  5,
		HandlerSuccessTotal: 4,
		HandlerFailureTotal: 1,
		RetryTotal:          1,
		LastExecutionAt:     exec.Format(time.RFC3339),
		LastSuccessAt:       time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
		LastFailureAt:       time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339),
		LastDLQAt:           time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339),
	}
	c.RecordAppStats(in)

	s, ok := c.AppStats("alpha")
	if !ok {
		t.Fatal("expected function stats row after record")
	}
	in.UpdatedAt = s.UpdatedAt
	if s != in {
		t.Fatalf("function stats = %+v, want %+v", s, in)
	}
	// Every persisted timestamp parses as RFC3339 (same convention as
	// updated_at).
	for name, v := range map[string]string{
		"LastExecutionAt": s.LastExecutionAt,
		"LastSuccessAt":   s.LastSuccessAt,
		"LastFailureAt":   s.LastFailureAt,
		"LastDLQAt":       s.LastDLQAt,
	} {
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			t.Fatalf("%s not RFC3339: %q: %v", name, v, err)
		}
	}
}

// TestRecordAppStatsEmptyTimestampPreservesExisting pins the CASE guard:
// an empty incoming timestamp PRESERVES the previously stored one — "no
// observation" never erases "last observed at" — and a NEW value overwrites.
func TestRecordAppStatsEmptyTimestampPreservesExisting(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-time.Hour).UTC()
	c.RecordAppStats(AppStats{
		App:                "alpha",
		EventsMatchedTotal: 1,
		LastExecutionAt:    exec.Format(time.RFC3339),
		LastDLQAt:          exec.Format(time.RFC3339),
	})

	// A later flush carries counters but NO timestamps: both must survive.
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 2})
	s, ok := c.AppStats("alpha")
	if !ok || s.EventsMatchedTotal != 2 {
		t.Fatalf("alpha = %+v, ok=%v; want events 2 (counters replaced)", s, ok)
	}
	if s.LastExecutionAt != exec.Format(time.RFC3339) || s.LastDLQAt != exec.Format(time.RFC3339) {
		t.Fatalf("empty incoming timestamps must preserve persisted ones: %+v", s)
	}

	// A NEW value does overwrite (latest wins).
	reExec := time.Now().UTC()
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 3, LastExecutionAt: reExec.Format(time.RFC3339)})
	s, _ = c.AppStats("alpha")
	if s.LastExecutionAt != reExec.Format(time.RFC3339) {
		t.Fatalf("newer execution timestamp must overwrite: %+v", s)
	}
	// ...while the untouched empty one is still preserved.
	if s.LastDLQAt != exec.Format(time.RFC3339) {
		t.Fatalf("untouched DLQ timestamp must survive the overwrite: %+v", s)
	}
}

// TestRecordStatsSnapshotTimestampsPersist verifies RecordStatsSnapshot's
// per-app upsert persists timestamps and applies the same empty-preserves
// CASE guards.
func TestRecordStatsSnapshotTimestampsPersist(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	exec := time.Now().Add(-time.Minute).UTC()
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsMatchedTotal: 1},
		[]AppStats{{App: "alpha", EventsMatchedTotal: 1, LastExecutionAt: exec.Format(time.RFC3339)}}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// A follow-up flush carries NO timestamps: the stored one must survive.
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsMatchedTotal: 2},
		[]AppStats{{App: "alpha", EventsMatchedTotal: 2}}); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	s, ok := c.AppStats("alpha")
	if !ok || s.EventsMatchedTotal != 2 {
		t.Fatalf("alpha = %+v, ok=%v; want events 2", s, ok)
	}
	if s.LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("snapshot empty timestamp must preserve persisted value: %+v", s)
	}
}

// TestAllAppStatsTimestamps verifies AllAppStats reads the timestamp
// columns alongside the counters.
func TestAllAppStatsTimestamps(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().UTC()
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 1, LastExecutionAt: exec.Format(time.RFC3339)})
	c.RecordAppStats(AppStats{App: "beta", EventsMatchedTotal: 2})

	all := c.AllAppStats()
	if len(all) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(all), all)
	}
	if all[0].App != "alpha" || all[0].LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("alpha = %+v, want execution timestamp %q", all[0], all[0].LastExecutionAt)
	}
	if all[1].App != "beta" || all[1].LastExecutionAt != "" {
		t.Fatalf("beta = %+v, want empty timestamps", all[1])
	}
}

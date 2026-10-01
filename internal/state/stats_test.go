package state

import (
	"path/filepath"
	"testing"
	"time"
)

// RecordStats then Stats round-trips exact values, including a non-empty
// updated_at set by the write.
func TestStatsRoundTrip(t *testing.T) {
	c := openTestState(t)
	in := Stats{
		EventsReceivedTotal:     20,
		EventsMatchedTotal:      12,
		EventsUnmatchedTotal:    8,
		HandlerSuccessTotal:     9,
		HandlerFailureTotal:     3,
		RetryTotal:              2,
		DLQTotal:                1,
		PendingEntries:          5,
		OldestPendingAgeSeconds: 42,
	}
	c.RecordStats(in)

	s, ok := c.Stats()
	if !ok {
		t.Fatal("expected stats row after record")
	}
	if s.UpdatedAt == "" {
		t.Fatal("expected non-empty updated_at")
	}
	if _, err := time.Parse(time.RFC3339, s.UpdatedAt); err != nil {
		t.Fatalf("updated_at not RFC3339: %q: %v", s.UpdatedAt, err)
	}
	// Compare all fields except updated_at, which is written by RecordStats.
	in.UpdatedAt = s.UpdatedAt
	if s != in {
		t.Fatalf("stats = %+v, want %+v", s, in)
	}
}

// A second RecordStats REPLACES every column (gauge semantics): pending_entries
// goes 3 -> 7 -> 0 across writes rather than accumulating.
func TestRecordStatsReplaces(t *testing.T) {
	c := openTestState(t)

	c.RecordStats(Stats{PendingEntries: 3})
	// Counters are replaced too, not accumulated.
	c.RecordStats(Stats{PendingEntries: 7, EventsMatchedTotal: 100})

	s, ok := c.Stats()
	if !ok {
		t.Fatal("expected stats row")
	}
	if s.PendingEntries != 7 {
		t.Fatalf("pending = %d, want 7", s.PendingEntries)
	}
	if s.EventsMatchedTotal != 100 {
		t.Fatalf("events = %d, want 100", s.EventsMatchedTotal)
	}

	c.RecordStats(Stats{PendingEntries: 0, EventsMatchedTotal: 100})
	s, ok = c.Stats()
	if !ok {
		t.Fatal("expected stats row")
	}
	if s.PendingEntries != 0 {
		t.Fatalf("pending = %d, want 0 after reset", s.PendingEntries)
	}
	if s.EventsMatchedTotal != 100 {
		t.Fatalf("events = %d, want 100 preserved", s.EventsMatchedTotal)
	}
}

// Stats on an empty DB yields (zero, false).
func TestStatsOnEmptyDB(t *testing.T) {
	c := openTestState(t)
	s, ok := c.Stats()
	if ok {
		t.Fatalf("expected absent, got %+v", s)
	}
	if s != (Stats{}) {
		t.Fatalf("expected zero stats, got %+v", s)
	}
}

// RebuildFromFS does not erase the stats row: discovery only touches the
// apps table.
func TestRebuildFromFSKeepsStats(t *testing.T) {
	root := t.TempDir()
	writeAppsDir(t, root)

	c := openTestState(t)
	c.RecordStats(Stats{EventsMatchedTotal: 55, PendingEntries: 3})

	if err := c.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	s, ok := c.Stats()
	if !ok {
		t.Fatal("stats row must survive rebuild")
	}
	if s.EventsMatchedTotal != 55 || s.PendingEntries != 3 {
		t.Fatalf("stats changed by rebuild: %+v", s)
	}
}

// RecordAppStats then AppStats round-trips exact values, including a
// non-empty updated_at set by the write.
func TestAppStatsRoundTrip(t *testing.T) {
	c := openTestState(t)
	in := AppStats{
		App:                 "alpha",
		EventsMatchedTotal:  5,
		HandlerSuccessTotal: 4,
		HandlerFailureTotal: 1,
		RetryTotal:          1,
		DLQTotal:            0,
	}
	c.RecordAppStats(in)

	s, ok := c.AppStats("alpha")
	if !ok {
		t.Fatal("expected function stats row after record")
	}
	if s.App != "alpha" {
		t.Fatalf("function = %q, want alpha", s.App)
	}
	if s.UpdatedAt == "" {
		t.Fatal("expected non-empty updated_at")
	}
	if _, err := time.Parse(time.RFC3339, s.UpdatedAt); err != nil {
		t.Fatalf("updated_at not RFC3339: %q: %v", s.UpdatedAt, err)
	}
	// Compare all fields except updated_at, which is written by RecordAppStats.
	in.UpdatedAt = s.UpdatedAt
	if s != in {
		t.Fatalf("function stats = %+v, want %+v", s, in)
	}
}

// Two apps keep independent rows: recording one does not affect the other.
func TestAppStatsIndependentRows(t *testing.T) {
	c := openTestState(t)
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 3})
	c.RecordAppStats(AppStats{App: "beta", EventsMatchedTotal: 7})

	a, ok := c.AppStats("alpha")
	if !ok || a.EventsMatchedTotal != 3 {
		t.Fatalf("alpha = %+v, ok=%v; want events 3", a, ok)
	}
	b, ok := c.AppStats("beta")
	if !ok || b.EventsMatchedTotal != 7 {
		t.Fatalf("beta = %+v, ok=%v; want events 7", b, ok)
	}
}

// A second RecordAppStats REPLACES every counter column (gauge semantics),
// not accumulates.
func TestAppStatsUpdateReplaces(t *testing.T) {
	c := openTestState(t)
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 3, HandlerSuccessTotal: 2})
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 9, HandlerSuccessTotal: 8})

	s, ok := c.AppStats("alpha")
	if !ok {
		t.Fatal("expected function stats row")
	}
	if s.EventsMatchedTotal != 9 {
		t.Fatalf("events = %d, want 9 (replaced, not accumulated)", s.EventsMatchedTotal)
	}
	if s.HandlerSuccessTotal != 8 {
		t.Fatalf("success = %d, want 8", s.HandlerSuccessTotal)
	}
}

// AppStats on an absent app yields (zero, false).
func TestAppStatsAbsent(t *testing.T) {
	c := openTestState(t)
	s, ok := c.AppStats("ghost")
	if ok {
		t.Fatalf("expected absent, got %+v", s)
	}
	if s != (AppStats{}) {
		t.Fatalf("expected zero function stats, got %+v", s)
	}
}

// RecordRemoved deletes the app_stats row alongside the apps row.
func TestRemovalDeletesAppStats(t *testing.T) {
	c := openTestState(t)
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 3})

	c.RecordRemoved("alpha")

	if _, ok := c.AppStats("alpha"); ok {
		t.Fatal("expected function stats row to be removed")
	}
}

// Schema init is idempotent: re-opening the same path succeeds and the stats
// table is present alongside the apps table.
func TestOpenWithStatsSchemaIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	c1.RecordStats(Stats{RetryTotal: 4})
	_ = c1.Close()

	c2, err := Open(path) // re-open, init (including new table) must be idempotent
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	s, ok := c2.Stats()
	if !ok {
		t.Fatal("expected stats row after reopen")
	}
	if s.RetryTotal != 4 {
		t.Fatalf("retry = %d, want 4 preserved across reopen", s.RetryTotal)
	}
}

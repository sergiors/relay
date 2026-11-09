//go:build integration

package state

import (
	"context"
	"errors"
	"testing"
)

// This file pins the STRICT stats reader contract ReadStats/ReadAppStats
// expose to the CLI: absence (no rows recorded yet) is (ok=false, nil error)
// and reads as legitimate data, while a failed read — a query error, scan
// error, or payload decode error — is (ok=false, error) and must never be
// mistaken for absence. The best-effort wrappers (Stats/AppStats) collapse the
// two outcomes, so only these readers can prove the distinction.

// TestReadStatsAbsentRowIsNotError pins the absence contract: a stats table
// with no rows reports ok=false, nil error, and the zero snapshot, so a caller
// renders a fresh database's zeros deliberately rather than by default.
func TestReadStatsAbsentRowIsNotError(t *testing.T) {
	c := openTestState(t)

	s, ok, err := c.ReadStats(context.Background())
	if err != nil {
		t.Fatalf("ReadStats on an empty database: %v", err)
	}
	if ok {
		t.Fatal("ReadStats on an empty database must report ok=false")
	}
	if s != (Stats{}) {
		t.Fatalf("ReadStats on an empty database = %+v, want the zero snapshot", s)
	}
}

// TestReadStatsRoundTrip pins that the strict reader returns exactly the
// recorded counters with ok=true — the same values the best-effort Stats read
// reports, without the logging shortcut.
func TestReadStatsRoundTrip(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{EventsReceivedTotal: 20, EventsMatchedTotal: 12, DLQTotal: 1})

	s, ok, err := c.ReadStats(context.Background())
	if err != nil {
		t.Fatalf("ReadStats: %v", err)
	}
	if !ok {
		t.Fatal("ReadStats after a record must report ok=true")
	}
	if s.EventsReceivedTotal != 20 || s.EventsMatchedTotal != 12 || s.DLQTotal != 1 {
		t.Fatalf("ReadStats = %+v, want the recorded counters", s)
	}
	if s.UpdatedAt == "" {
		t.Fatalf("ReadStats = %+v, want a non-empty UpdatedAt", s)
	}
}

// TestReadStatsCancelledContextFails pins the distinction in the direction the
// CLI's exit code depends on: a cancelled (or otherwise failed) read is an
// error alongside ok=false — never folded into the legitimate absent outcome,
// which would render an unreadable database as a real all-zero snapshot.
func TestReadStatsCancelledContextFails(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{EventsReceivedTotal: 20})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the read begins

	s, ok, err := c.ReadStats(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadStats on a cancelled context: err = %v, want context.Canceled", err)
	}
	if ok {
		t.Fatal("a failed read must not report ok=true")
	}
	if s != (Stats{}) {
		t.Fatalf("a failed read must return the zero snapshot, got %+v", s)
	}
}

// TestReadStatsCorruptPayloadFails pins the decode branch of the same
// contract: a row that is valid JSONB but not the typed payload shape is a
// returned error with ok=false, never absence masquerading as "never recorded"
// and never a silent zero snapshot.
func TestReadStatsCorruptPayloadFails(t *testing.T) {
	c := openTestState(t)
	// The column is NOT NULL JSONB, so corruption is a well-formed value of
	// the wrong shape: a bare scalar decodes into the typed struct nowhere.
	if _, err := c.db.ExecContext(context.Background(),
		`INSERT INTO stats (node_id, instance, data, updated_at) VALUES ('n1', '-', '123', '0001-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed corrupt stats row: %v", err)
	}

	s, ok, err := c.ReadStats(context.Background())
	if err == nil {
		t.Fatalf("ReadStats on a corrupt payload must fail, got ok=%v snapshot=%+v", ok, s)
	}
	if ok {
		t.Fatal("a failed decode must not report ok=true")
	}
	if s != (Stats{}) {
		t.Fatalf("a failed decode must return the zero snapshot, got %+v", s)
	}
}

// TestReadAppStatsAbsentRowIsNotError pins the strict per-app reader's absence
// contract: no app_stats rows for the name is (ok=false, nil error) and the
// zero snapshot with NO app attributed.
func TestReadAppStatsAbsentRowIsNotError(t *testing.T) {
	c := openTestState(t)

	fs, ok, err := c.ReadAppStats(context.Background(), "ghost")
	if err != nil {
		t.Fatalf("ReadAppStats for an unknown app: %v", err)
	}
	if ok {
		t.Fatal("ReadAppStats with no rows must report ok=false")
	}
	if fs != (AppStats{}) {
		t.Fatalf("ReadAppStats with no rows = %+v, want the zero snapshot", fs)
	}
}

// TestReadAppStatsRoundTrip pins that the strict per-app reader returns the
// recorded counters and attributes them to the requested app.
func TestReadAppStatsRoundTrip(t *testing.T) {
	c := openTestState(t)
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 5, WarmAcquiresTotal: 2})

	fs, ok, err := c.ReadAppStats(context.Background(), "alpha")
	if err != nil {
		t.Fatalf("ReadAppStats: %v", err)
	}
	if !ok {
		t.Fatal("ReadAppStats after a record must report ok=true")
	}
	if fs.EventsMatchedTotal != 5 || fs.WarmAcquiresTotal != 2 {
		t.Fatalf("ReadAppStats = %+v, want the recorded counters", fs)
	}
	if fs.App != "alpha" {
		t.Fatalf("ReadAppStats must attribute the snapshot to the app, got App=%q", fs.App)
	}
}

// TestReadAppStatsCancelledContextFails pins the failed-read branch of the
// per-app reader: an error is returned (wrapped context cancellation), ok
// stays false, and nothing is presented as a zero snapshot.
func TestReadAppStatsCancelledContextFails(t *testing.T) {
	c := openTestState(t)
	c.RecordAppStats(AppStats{App: "alpha", EventsMatchedTotal: 5})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the read begins

	fs, ok, err := c.ReadAppStats(ctx, "alpha")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAppStats on a cancelled context: err = %v, want context.Canceled", err)
	}
	if ok {
		t.Fatal("a failed read must not report ok=true")
	}
	if fs != (AppStats{}) {
		t.Fatalf("a failed read must return the zero snapshot, got %+v", fs)
	}
}

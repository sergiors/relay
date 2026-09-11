package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/state"
)

// seedStatsState creates a temp state DB, redirects the package CLI path
// (statePath) to it, and records a known global stats snapshot. It returns the
// opened state DB; later command calls read the same path.
func seedStatsState(t *testing.T) *state.State {
	t.Helper()
	statePath = filepath.Join(t.TempDir(), "db.sqlite3")
	st, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	st.RecordStats(state.Stats{
		EventsProcessedTotal:    152934,
		HandlerSuccessTotal:     152801,
		HandlerFailureTotal:     133,
		RetryTotal:              82,
		DLQTotal:                4,
		PendingEntries:          17,
		OldestPendingAgeSeconds: 134,
	})
	return st
}

// printStats renders the recorded snapshot with tab-aligned labels, Go-duration
// age formatting, and a relative Updated timestamp.
func TestPrintStats(t *testing.T) {
	st := seedStatsState(t)
	var w bytes.Buffer
	printStats(&w, st)
	out := w.String()

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 8 {
		t.Fatalf("expected 8 lines, got %d:\n%s", len(lines), out)
	}

	// "Oldest pending age:" and "Pending entries:" are the longest labels, so
	// tabwriter pads values to a common column; assert the exact aligned rows.
	wants := []string{
		"Events processed:    152934",
		"Handler successes:   152801",
		"Handler failures:    133",
		"Retries:             82",
		"DLQ entries:         4",
		"Pending entries:     17",
		"Oldest pending age:  2m14s",
	}
	for i, want := range wants {
		if lines[i] != want {
			t.Errorf("row %d want %q, got %q", i, want, lines[i])
		}
	}
	// Updated is a just-recorded row, so it is "just now".
	if !strings.Contains(lines[7], "Updated:") || !strings.Contains(lines[7], "ago") {
		t.Errorf("row 7 want Updated + relative age, got %q", lines[7])
	}
}

// humanAge formats integer seconds as Go durations, with zero rendered as "0s".
func TestHumanAge(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{
		{0, "0s"},
		{134, "2m14s"},
		{3723, "1h2m3s"},
	} {
		if got := humanAge(tc.in); got != tc.want {
			t.Errorf("humanAge(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A fresh state DB has no stats row; printStats renders all zeros, "0s" age,
// and "never" for Updated, and the command exits 0.
func TestPrintStatsEmptyDB(t *testing.T) {
	statePath = filepath.Join(t.TempDir(), "db.sqlite3")
	st, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	var w bytes.Buffer
	printStats(&w, st)
	out := w.String()
	for _, want := range []string{
		"Events processed:    0",
		"Handler successes:   0",
		"Handler failures:    0",
		"Retries:             0",
		"DLQ entries:         0",
		"Pending entries:     0",
		"Oldest pending age:  0s",
		"Updated:             never",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}

	if _, _, err := runCLI(t, "", "stats"); err != nil {
		t.Fatalf("stats on empty DB: err = %v, want nil", err)
	}
}

// `relay stats` reads the seeded snapshot and exits 0; `relay stats extra` is a
// usage error exiting 2; `relay stats --help` prints usage to stdout and exits 0.
func TestStatsCommand(t *testing.T) {
	seedStatsState(t)
	if _, _, err := runCLI(t, "", "stats"); err != nil {
		t.Fatalf("stats: err = %v, want nil", err)
	}

	if _, _, err := runCLI(t, "", "stats", "extra"); err == nil || !strings.Contains(err.Error(), "stats: too many arguments") {
		t.Fatalf("stats extra: missing rejection error: %v", err)
	}

	out, _, err := runCLI(t, "", "stats", "--help")
	if err != nil {
		t.Fatalf("stats --help: err = %v, want nil", err)
	}
	if !strings.Contains(out, "stats") {
		t.Fatalf("stats --help: stdout missing stats usage:\n%s", out)
	}
}

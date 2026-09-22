package cli

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/worker"
)

// fakeStatsResetter counts ResetStats calls so a CLI test can prove the socket
// path invoked the worker's resetter.
type fakeStatsResetter struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeStatsResetter) ResetStats() error {
	f.mu.Lock()
	f.calls++
	err := f.err
	f.mu.Unlock()
	return err
}

func (f *fakeStatsResetter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// startStatsSocket starts a worker query socket at path backed by no pools and
// the given stats resetter, so the CLI's running-worker reset path is exercised
// against a real socket.
func startStatsSocket(t *testing.T, path string, resetter worker.StatsResetter) {
	t.Helper()
	s, err := worker.NewSocketServer(
		path,
		fakePoolSnapshotter{pools: map[string]runtime.PoolSnapshot{}},
		resetter,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
}

// seedStatsState creates a temp state DB under test dependencies and records a
// known global stats snapshot. It returns the opened state DB and the deps
// whose StatePath points at it, so command tests run with runCLIWithDeps read
// exactly what the test seeded.
func seedStatsState(t *testing.T) (*state.State, Dependencies) {
	t.Helper()
	st, deps := openTempState(t)

	st.RecordStats(state.Stats{
		EventsProcessedTotal:    152934,
		HandlerSuccessTotal:     152801,
		HandlerFailureTotal:     133,
		RetryTotal:              82,
		DLQTotal:                4,
		PendingEntries:          17,
		OldestPendingAgeSeconds: 134,
	})
	return st, deps
}

// printStats renders the recorded snapshot with tab-aligned labels, Go-duration
// age formatting, and a relative Updated timestamp.
func TestPrintStats(t *testing.T) {
	st, _ := seedStatsState(t)
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

// TestHumanAge pins Go-duration formatting, including zero rendered as "0s".
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
	st, deps := openTempState(t)

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

	if _, _, err := runCLIWithDeps(t, deps, "", "stats"); err != nil {
		t.Fatalf("stats on empty DB: err = %v, want nil", err)
	}
}

// `relay stats` reads the seeded snapshot and exits 0; `relay stats extra` is a
// usage error exiting 2; `relay stats --help` prints usage to stdout and exits 0.
func TestStatsCommand(t *testing.T) {
	_, deps := seedStatsState(t)
	if _, _, err := runCLIWithDeps(t, deps, "", "stats"); err != nil {
		t.Fatalf("stats: err = %v, want nil", err)
	}

	if _, _, err := runCLIWithDeps(t, deps, "", "stats", "extra"); err == nil ||
		!strings.Contains(err.Error(), "stats: too many arguments") {
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

// `relay stats reset` with no running worker falls back to the state database:
// it prints the exact confirmation line, zeroes the global cumulative counters
// and every function_stats row IN PLACE, but PRESERVES the live backlog gauges.
// A following bare `relay stats` renders zeros for the counters and the
// preserved gauge value.
func TestStatsResetCommand(t *testing.T) {
	st, deps := seedStatsState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 9,
		WarmAcquiresTotal:    3,
		LastExecutionAt:      time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	})

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err != nil {
		t.Fatalf("stats reset: err = %v, want nil", err)
	}
	if out != "Stats reset\n" {
		t.Fatalf("stats reset stdout = %q, want %q", out, "Stats reset\n")
	}

	// Persisted state: global counters zeroed, gauges preserved, function rows
	// reset in place (not deleted).
	s, ok := st.Stats()
	if !ok {
		t.Fatal("stats row must survive the reset")
	}
	if s.EventsProcessedTotal != 0 || s.HandlerSuccessTotal != 0 ||
		s.HandlerFailureTotal != 0 || s.RetryTotal != 0 || s.DLQTotal != 0 {
		t.Fatalf("cumulative counters must be zeroed: %+v", s)
	}
	if s.PendingEntries != 17 || s.OldestPendingAgeSeconds != 134 {
		t.Fatalf("backlog gauges must be preserved: %+v", s)
	}
	all := st.AllFunctionStats()
	if len(all) != 1 || all[0].Function != "alpha" {
		t.Fatalf("function_stats rows must be preserved: %+v", all)
	}
	if all[0].EventsProcessedTotal != 0 || all[0].WarmAcquiresTotal != 0 || all[0].LastExecutionAt != "" {
		t.Fatalf("function_stats must be zeroed in place: %+v", all[0])
	}

	// A follow-up bare `relay stats` still renders the table (the parent Action
	// with no subcommand token), now with zero counters and the preserved gauge.
	rendered, _, err := runCLIWithDeps(t, deps, "", "stats")
	if err != nil {
		t.Fatalf("stats after reset: err = %v, want nil", err)
	}
	for _, want := range []string{
		"Events processed:    0",
		"Handler successes:   0",
		"Handler failures:    0",
		"Retries:             0",
		"DLQ entries:         0",
		"Pending entries:     17",
		"Oldest pending age:  2m14s",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("post-reset stats output missing %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "COMMANDS:") {
		t.Errorf("bare stats must render the table, not help:\n%s", rendered)
	}
}

// `relay stats reset` surfaces a worker-reported reset failure instead of
// silently falling back to the direct state write (which would mask it).
func TestStatsResetCommandWorkerFailureSurfaces(t *testing.T) {
	st, deps := seedStatsState(t)
	resetter := &fakeStatsResetter{err: errors.New("state unavailable")}
	startStatsSocket(t, deps.SocketPath, resetter)

	_, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err == nil || !strings.Contains(err.Error(), "stats reset") {
		t.Fatalf("worker failure must surface as an error, got %v", err)
	}
	// The CLI must not have written the state DB as a silent fallback.
	s, _ := st.Stats()
	if s.EventsProcessedTotal != 152934 {
		t.Fatalf("failed worker reset must not fall back to a state write: %+v", s)
	}
}

// `relay stats reset` with a RUNNING worker resets through the worker's socket,
// so the worker's in-memory totals are reset too, and the CLI does NOT fall back
// to the direct state write. The state DB is left untouched by the CLI itself
// (the worker owns the persisted reset).
func TestStatsResetCommandRunningWorker(t *testing.T) {
	st, deps := seedStatsState(t)
	// A worker-owned state DB is a separate handle; the CLI's direct fallback
	// would write here, so an untouched row proves the socket path was taken.
	resetter := &fakeStatsResetter{}
	startStatsSocket(t, deps.SocketPath, resetter)

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err != nil {
		t.Fatalf("stats reset: err = %v, want nil", err)
	}
	if out != "Stats reset\n" {
		t.Fatalf("stats reset stdout = %q, want %q", out, "Stats reset\n")
	}
	if resetter.count() != 1 {
		t.Fatalf("worker resetter calls = %d, want 1 (socket path)", resetter.count())
	}
	// The CLI must not have written the state DB directly.
	s, _ := st.Stats()
	if s.EventsProcessedTotal != 152934 {
		t.Fatalf("CLI must not reset the state DB when a worker answered: %+v", s)
	}
}

// `relay stats reset extra` is a usage error exiting 2, and the rejection
// happens before any state write.
func TestStatsResetTooManyArgs(t *testing.T) {
	st, deps := seedStatsState(t)

	_, _, err := runCLIWithDeps(t, deps, "", "stats", "reset", "extra")
	if err == nil || !strings.Contains(err.Error(), "stats reset: too many arguments") {
		t.Fatalf("stats reset extra: missing rejection error: %v", err)
	}
	s, ok := st.Stats()
	if !ok || s.EventsProcessedTotal != 152934 {
		t.Fatalf("rejected reset must not write: %+v, ok=%v", s, ok)
	}
}

// A junk token under `stats` is NOT swallowed by the new subcommand list: with
// no matching subcommand, urfave runs the parent Action, whose too-many-
// arguments guard preserves the previous `stats: too many arguments` semantics.
func TestStatsUnknownTokenFallsThroughToParentGuard(t *testing.T) {
	_, deps := seedStatsState(t)

	_, _, err := runCLIWithDeps(t, deps, "", "stats", "asdsa")
	if err == nil || !strings.Contains(err.Error(), "stats: too many arguments") {
		t.Fatalf("stats asdsa: err = %v, want parent guard", err)
	}
}

package cli

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"

	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/worker"
)

// fakePoolSnapshotter is an in-memory worker.PoolSnapshotter for CLI tests, so
// the live query socket can be exercised without Docker or a runtime.Manager.
type fakePoolSnapshotter struct {
	pools map[string]runtime.PoolSnapshot
}

func (f fakePoolSnapshotter) PoolSnapshot(name string) (runtime.PoolSnapshot, bool) {
	s, ok := f.pools[name]
	return s, ok
}

// startTestSocketServer starts a real worker query socket at path backed by
// pools. The socket path comes from the test's injected Dependencies, so the
// CLI's socket call is exercised end to end without a package-global path seam.
func startTestSocketServer(t *testing.T, path string, pools map[string]runtime.PoolSnapshot) {
	t.Helper()
	s, err := worker.NewSocketServer(
		path,
		fakePoolSnapshotter{pools: pools},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
}

// withProviderDisabled clears the in-process provider for the duration of the
// test, so inspect falls through to the worker query socket. It is the one
// remaining non-path test seam.
func withProviderDisabled(t *testing.T) {
	t.Helper()
	orig := poolSnapshotProvider
	SetPoolSnapshotProvider(nil)
	t.Cleanup(func() { SetPoolSnapshotProvider(orig) })
}

// TestFunctionInspectRuntimePoolSection verifies that when a live pool provider
// is wired, inspect renders the compact Runtime pool section with the live
// gauges (capacity and container counts) from the provider, while the cumulative
// acquire/discard counters STILL come from the persisted stats row — the
// provider's own counter fields are ignored. The provider counters are
// deliberately set to different values than the persisted row so the test
// fails if the renderer ever prefers the live counters.
func TestFunctionInspectRuntimePoolSection(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:          "user-events-python",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}

	orig := poolSnapshotProvider
	SetPoolSnapshotProvider(func(name string) (runtime.PoolSnapshot, bool) {
		if name != "user-events-python" {
			return runtime.PoolSnapshot{}, false
		}
		return runtime.PoolSnapshot{
			Function:   name,
			Capacity:   4,
			Containers: 2,
			Busy:       1,
			Idle:       1,
			Starting:   0,
			// Deliberately different from the persisted stats: the CLI must
			// ignore these and render the persisted row.
			WarmAcquires: 700,
			ColdStarts:   300,
			Discarded:    200,
		}, true
	})
	defer SetPoolSnapshotProvider(orig)

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	for _, want := range []string{
		"Runtime pool:",
		"Capacity:",
		"Containers:",
		"Busy:",
		"Idle:",
		"Warm acquires:",
		"Cold starts:",
		"Discarded:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// Live gauges come from the provider; cumulative counters come from the
	// persisted stats row.
	for _, row := range []string{
		"Capacity:        4",
		"Containers:      2",
		"Busy:            1",
		"Idle:            1",
		"Warm acquires:   7",
		"Cold starts:     3",
		"Discarded:       2",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("inspect output missing value row %q\n%s", row, out)
		}
	}
	// Starting is zero, so the transient row is omitted.
	if strings.Contains(out, "Starting:") {
		t.Errorf("Starting row must be omitted when zero:\n%s", out)
	}
}

// TestFunctionInspectRuntimePoolStartingRow verifies the Starting row appears
// only when an in-flight lazy start is observed.
func TestFunctionInspectRuntimePoolStartingRow(t *testing.T) {
	var w bytes.Buffer
	printRuntimePool(&w, &poolGauges{Capacity: 2, Starting: 1}, 0, 0, 0)
	out := w.String()
	if !strings.Contains(out, "Starting:") || !strings.Contains(out, "1") {
		t.Errorf("starting row missing:\n%s", out)
	}
}

// TestFunctionInspectRuntimePoolPersistedCountersWithoutProvider verifies the
// standalone inspect process (no provider wired, no worker socket) renders the
// Runtime pool section from the persisted cumulative counters while marking the
// live gauges explicitly unavailable — never inventing stale live state.
func TestFunctionInspectRuntimePoolPersistedCountersWithoutProvider(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:          "user-events-python",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}

	withProviderDisabled(t)

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	if !strings.Contains(out, "Runtime pool:") {
		t.Fatalf("standalone inspect must render the Runtime pool section from persisted counters:\n%s", out)
	}
	// The persisted cumulative counters are rendered exactly.
	for _, row := range []string{
		"Warm acquires:   7",
		"Cold starts:     3",
		"Discarded:       2",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("inspect output missing persisted counter row %q\n%s", row, out)
		}
	}
	// The live gauges are explicitly unavailable, never a guessed/stale number.
	for _, row := range []string{
		"Capacity:        unknown",
		"Containers:      unknown",
		"Busy:            unknown",
		"Idle:            unknown",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("inspect output missing unavailable gauge row %q\n%s", row, out)
		}
	}
}

// TestFunctionInspectRuntimePoolProviderUnknownFunction verifies a provider that
// reports not-found still renders the persisted-counter shape (the state record
// exists, only the live pool is unreachable).
func TestFunctionInspectRuntimePoolProviderUnknownFunction(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:          "user-events-python",
		WarmAcquiresTotal: 4,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}

	orig := poolSnapshotProvider
	SetPoolSnapshotProvider(func(string) (runtime.PoolSnapshot, bool) { return runtime.PoolSnapshot{}, false })
	defer SetPoolSnapshotProvider(orig)

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	if !strings.Contains(out, "Runtime pool:") || !strings.Contains(out, "Warm acquires:   4") {
		t.Fatalf("not-found provider must still render persisted counters:\n%s", out)
	}
	if !strings.Contains(out, "Capacity:        unknown") {
		t.Fatalf("not-found provider must mark live gauges unavailable:\n%s", out)
	}
}

// TestFunctionInspectCommandRendersPersistedPoolCounters is the end-to-end
// command-path check: the standalone `relay function inspect NAME` command
// renders the Runtime pool section from the persisted stats row with a zero
// counters fallback when no pool activity has been recorded and no worker socket
// is reachable.
func TestFunctionInspectCommandRendersPersistedPoolCounters(t *testing.T) {
	_, deps := seedTestState(t)
	withProviderDisabled(t)
	out, _, err := runCLIWithDeps(t, deps, "", "function", "inspect", "user-events-python")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "Runtime pool:") {
		t.Fatalf("standalone inspect must render a Runtime pool section:\n%s", out)
	}
	if !strings.Contains(out, "Warm acquires:   0") || !strings.Contains(out, "Capacity:        unknown") {
		t.Fatalf("standalone inspect must render zero persisted counters and unknown gauges:\n%s", out)
	}
}

// TestFunctionInspectSocketLiveGauges verifies the standalone CLI path: with the
// in-process provider absent and a worker socket answering, inspect renders the
// live gauges from the socket while the cumulative counters still come from the
// persisted stats row.
func TestFunctionInspectSocketLiveGauges(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:          "user-events-python",
		WarmAcquiresTotal: 11,
		ColdStartsTotal:   4,
		DiscardedTotal:    1,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	withProviderDisabled(t)
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"user-events-python": {Function: "user-events-python", Capacity: 4, Containers: 2, Busy: 1, Idle: 1},
	})

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	for _, row := range []string{
		"Capacity:        4",
		"Containers:      2",
		"Busy:            1",
		"Idle:            1",
		// Cumulative counters remain the persisted values, not the socket's.
		"Warm acquires:   11",
		"Cold starts:     4",
		"Discarded:       1",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("socket-backed inspect missing %q\n%s", row, out)
		}
	}
	if strings.Contains(out, "unknown") {
		t.Errorf("socket-backed inspect must not render unknown gauges:\n%s", out)
	}
	if strings.Contains(out, "Starting:") {
		t.Errorf("Starting must be omitted when zero:\n%s", out)
	}
}

// TestFunctionInspectSocketKnownZeros verifies a live pool whose gauges are all
// valid zeros renders 0 (not unknown), pinning that "known" is keyed on the
// query succeeding rather than on non-zero values.
func TestFunctionInspectSocketKnownZeros(t *testing.T) {
	st, deps := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	withProviderDisabled(t)
	// An all-zero pool is a valid live reading: the function exists in the
	// worker's pool map with zero gauges.
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"user-events-python": {Function: "user-events-python"},
	})

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	for _, row := range []string{
		"Capacity:        0",
		"Containers:      0",
		"Busy:            0",
		"Idle:            0",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("known-zero inspect missing %q\n%s", row, out)
		}
	}
	if strings.Contains(out, "unknown") {
		t.Errorf("known-zero live gauges must render 0, not unknown:\n%s", out)
	}
}

// TestFunctionInspectSocketUnknownFunction verifies the worker's
// unknown-function answer renders the persisted-counter/unknown-gauge shape
// rather than an error, exactly like an unreachable socket.
func TestFunctionInspectSocketUnknownFunction(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:          "user-events-python",
		WarmAcquiresTotal: 5,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	withProviderDisabled(t)
	// The socket is live but serves a DIFFERENT function, so the query answers
	// unknown_function.
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"other": {Function: "other", Capacity: 1},
	})

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	if !strings.Contains(out, "Warm acquires:   5") || !strings.Contains(out, "Capacity:        unknown") {
		t.Fatalf("unknown-function answer must render persisted counters and unknown gauges:\n%s", out)
	}
}

// TestFunctionInspectSocketUnavailable verifies an unreachable socket (the
// standalone case with no worker running) renders unknown live gauges and the
// persisted counters.
func TestFunctionInspectSocketUnavailable(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:          "user-events-python",
		WarmAcquiresTotal: 6,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	withProviderDisabled(t)
	// No socket server is started at deps.SocketPath: the dial fails.

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs, deps.SocketPath)
	out := w.String()
	if !strings.Contains(out, "Warm acquires:   6") || !strings.Contains(out, "Capacity:        unknown") {
		t.Fatalf("unavailable socket must render persisted counters and unknown gauges:\n%s", out)
	}
}

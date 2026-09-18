package cli

import (
	"bytes"
	"strings"
	"testing"

	"relay/internal/runtime"
	"relay/internal/state"
)

// TestFunctionInspectRuntimePoolSection verifies that when a live pool provider
// is wired, inspect renders the compact Runtime pool section with capacity,
// container counts, and the cumulative acquire/discard counters from the live
// snapshot.
func TestFunctionInspectRuntimePoolSection(t *testing.T) {
	st := seedTestState(t)
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
			Function:     name,
			Capacity:     4,
			Containers:   2,
			Busy:         1,
			Idle:         1,
			Starting:     0,
			WarmAcquires: 7,
			ColdStarts:   3,
			Discarded:    2,
		}, true
	})
	defer SetPoolSnapshotProvider(orig)

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs)
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
	// The rendered values map to the snapshot fields, in order.
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
	printRuntimePool(&w, &runtime.PoolSnapshot{Function: "f", Capacity: 2, Starting: 1}, state.FunctionStats{})
	out := w.String()
	if !strings.Contains(out, "Starting:") || !strings.Contains(out, "1") {
		t.Errorf("starting row missing:\n%s", out)
	}
}

// TestFunctionInspectRuntimePoolPersistedCountersWithoutProvider verifies the
// standalone inspect process (no provider wired) renders the Runtime pool
// section from the persisted cumulative counters while marking the live gauges
// explicitly unavailable — never inventing stale live state.
func TestFunctionInspectRuntimePoolPersistedCountersWithoutProvider(t *testing.T) {
	st := seedTestState(t)
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
	SetPoolSnapshotProvider(nil)
	defer SetPoolSnapshotProvider(orig)

	var w bytes.Buffer
	fs := printInspect(&w, st, d)
	appendRuntimePool(&w, "user-events-python", fs)
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
	st := seedTestState(t)
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
	appendRuntimePool(&w, "user-events-python", fs)
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
// counters fallback when no pool activity has been recorded.
func TestFunctionInspectCommandRendersPersistedPoolCounters(t *testing.T) {
	_ = seedTestState(t)
	out, _, err := runCLI(t, "", "function", "inspect", "user-events-python")
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

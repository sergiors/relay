//go:build integration

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

// startTestSocketServer starts a real worker query socket at path backed by
// pools. The socket path comes from the test's injected Dependencies, so the
// CLI's socket call is exercised end to end without a package-global path seam.
func startTestSocketServer(t *testing.T, path string, pools map[string]runtime.PoolSnapshot) {
	t.Helper()
	s, err := worker.NewSocketServer(
		path,
		fakePoolSnapshotter{pools: pools},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
}

// TestAppInspectRuntimePoolSection verifies that when a live pool provider is
// wired into the command's Dependencies, inspect renders the compact Runtime
// pool section with the live gauges (capacity and container counts) from that
// provider, while the cumulative acquire/discard counters STILL come from the
// persisted stats row — the provider's own counter fields are ignored. A live
// socket holding deliberately different gauges proves both the provider's
// precedence over the socket AND that the counters come only from the persisted
// row; it fails if the renderer ever prefers the socket or the live counters.
func TestAppInspectRuntimePoolSection(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}

	// The socket holds different gauges (and serves a different pool state) so
	// any renderer regression that consults the socket before — or instead of —
	// the injected provider renders 40/20 instead of 4/2.
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"user-events-python": {App: "user-events-python", Capacity: 40, Containers: 20, Busy: 10, Idle: 10},
	})

	// Deliberately different gauges and counters than the socket and the
	// persisted stats: the CLI must take the gauges from here and the counters
	// from the persisted row.
	deps.PoolSnapshotProvider = func(name string) (runtime.PoolSnapshot, bool) {
		if name != "user-events-python" {
			return runtime.PoolSnapshot{}, false
		}
		return runtime.PoolSnapshot{
			App:          name,
			Capacity:     4,
			Containers:   2,
			Busy:         1,
			Idle:         1,
			Starting:     0,
			WarmAcquires: 700,
			ColdStarts:   300,
			Discarded:    200,
		}, true
	}

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
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
		"Capacity: 4",
		"Containers: 2",
		"Busy: 1",
		"Idle: 1",
		"Warm acquires: 7",
		"Cold starts: 3",
		"Discarded: 2",
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

// TestAppInspectRuntimePoolPersistedCountersWithoutProvider verifies the
// standalone inspect process (no provider wired, no worker socket) renders the
// Runtime pool section from the persisted cumulative counters while marking the
// live gauges explicitly unavailable — never inventing stale live state.
func TestAppInspectRuntimePoolPersistedCountersWithoutProvider(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	// The provider stays unwired (nil) and no worker socket is started, so
	// neither live source resolves a pool.

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
	if !strings.Contains(out, "Runtime pool:") {
		t.Fatalf("standalone inspect must render the Runtime pool section from persisted counters:\n%s", out)
	}
	// The persisted cumulative counters are rendered exactly.
	for _, row := range []string{
		"Warm acquires: 7",
		"Cold starts: 3",
		"Discarded: 2",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("inspect output missing persisted counter row %q\n%s", row, out)
		}
	}
	// The live gauges are explicitly unavailable, never a guessed/stale number.
	for _, row := range []string{
		"Capacity: unknown",
		"Containers: unknown",
		"Busy: unknown",
		"Idle: unknown",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("inspect output missing unavailable gauge row %q\n%s", row, out)
		}
	}
}

// TestAppInspectRuntimePoolProviderUnknownApp verifies a provider that
// reports not-found still renders the persisted-counter shape (the state record
// exists, only the live pool is unreachable).
func TestAppInspectRuntimePoolProviderUnknownApp(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 4,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}

	// The provider is wired but reports no live pool for the app.
	deps.PoolSnapshotProvider = noPoolProvider

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
	if !strings.Contains(out, "Runtime pool:") || !strings.Contains(out, "Warm acquires: 4") {
		t.Fatalf("not-found provider must still render persisted counters:\n%s", out)
	}
	if !strings.Contains(out, "Capacity: unknown") {
		t.Fatalf("not-found provider must mark live gauges unavailable:\n%s", out)
	}
}

// TestAppInspectCommandRendersPersistedPoolCounters is the end-to-end
// command-path check: the standalone `relay app inspect NAME` command
// renders the Runtime pool section from the persisted stats row with a zero
// counters fallback when the provider is unwired and no worker socket is
// reachable.
func TestAppInspectCommandRendersPersistedPoolCounters(t *testing.T) {
	// The seed leaves PoolSnapshotProvider nil and SocketPath unbound.
	_, deps := seedTestState(t)
	out, _, err := runCLIWithDeps(t, deps, "", "app", "inspect", "user-events-python")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	out = normWS(out)
	if !strings.Contains(out, "Runtime pool:") {
		t.Fatalf("standalone inspect must render a Runtime pool section:\n%s", out)
	}
	if !strings.Contains(out, "Warm acquires: 0") || !strings.Contains(out, "Capacity: unknown") {
		t.Fatalf("standalone inspect must render zero persisted counters and unknown gauges:\n%s", out)
	}
}

// TestAppInspectSocketLiveGauges verifies the standalone CLI path: with the
// in-process provider unwired (nil) and a worker socket answering, inspect
// renders the live gauges from the socket while the cumulative counters still
// come from the persisted stats row.
func TestAppInspectSocketLiveGauges(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 11,
		ColdStartsTotal:   4,
		DiscardedTotal:    1,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	// The provider stays unwired (nil): the standalone CLI resolves the gauges
	// from the socket.
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"user-events-python": {App: "user-events-python", Capacity: 4, Containers: 2, Busy: 1, Idle: 1},
	})

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
	for _, row := range []string{
		"Capacity: 4",
		"Containers: 2",
		"Busy: 1",
		"Idle: 1",
		// Cumulative counters remain the persisted values, not the socket's.
		"Warm acquires: 11",
		"Cold starts: 4",
		"Discarded: 1",
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

// TestAppInspectSocketKnownZeros verifies a live pool whose gauges are all
// valid zeros renders 0 (not unknown), pinning that "known" is keyed on the
// query succeeding rather than on non-zero values.
func TestAppInspectSocketKnownZeros(t *testing.T) {
	st, deps := seedTestState(t)
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	// The provider stays unwired (nil): the socket is the only live source.
	// An all-zero pool is a valid live reading: the app exists in the
	// worker's pool map with zero gauges.
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"user-events-python": {App: "user-events-python"},
	})

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
	for _, row := range []string{
		"Capacity: 0",
		"Containers: 0",
		"Busy: 0",
		"Idle: 0",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("known-zero inspect missing %q\n%s", row, out)
		}
	}
	if strings.Contains(out, "unknown") {
		t.Errorf("known-zero live gauges must render 0, not unknown:\n%s", out)
	}
}

// TestAppInspectSocketUnknownApp verifies the worker's
// unknown-app answer renders the persisted-counter/unknown-gauge shape
// rather than an error, exactly like an unreachable socket.
func TestAppInspectSocketUnknownApp(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 5,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	// The provider stays unwired (nil), and the socket is live but serves a
	// DIFFERENT app, so the query answers unknown_app.
	startTestSocketServer(t, deps.SocketPath, map[string]runtime.PoolSnapshot{
		"other": {App: "other", Capacity: 1},
	})

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
	if !strings.Contains(out, "Warm acquires: 5") || !strings.Contains(out, "Capacity: unknown") {
		t.Fatalf("unknown-function answer must render persisted counters and unknown gauges:\n%s", out)
	}
}

// TestAppInspectSocketUnavailable verifies an unreachable socket (the
// standalone case with no worker running) renders unknown live gauges and the
// persisted counters.
func TestAppInspectSocketUnavailable(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 6,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	// The provider stays unwired (nil) and no socket server is started at
	// deps.SocketPath: the dial fails.

	var w bytes.Buffer
	fs := printInspect(&w, st, detail)
	appendRuntimePool(&w, "user-events-python", fs, deps.PoolSnapshotProvider, deps.SocketPath)
	out := normWS(w.String())
	if !strings.Contains(out, "Warm acquires: 6") || !strings.Contains(out, "Capacity: unknown") {
		t.Fatalf("unavailable socket must render persisted counters and unknown gauges:\n%s", out)
	}
}

// TestAppInspectCommandTreesCaptureIndependentProviders proves two constructed
// command trees use their own pool providers even if the original Dependencies
// values are changed after New. Persisted counters still come from state.
func TestAppInspectCommandTreesCaptureIndependentProviders(t *testing.T) {
	st, deps := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	})
	var callsA, callsB int
	depsA, depsB := deps, deps
	depsA.PoolSnapshotProvider = func(name string) (runtime.PoolSnapshot, bool) {
		callsA++
		if name != "user-events-python" {
			return runtime.PoolSnapshot{}, false
		}
		return runtime.PoolSnapshot{App: name, Capacity: 4, Containers: 2, Busy: 1, Idle: 1}, true
	}
	depsB.PoolSnapshotProvider = func(name string) (runtime.PoolSnapshot, bool) {
		callsB++
		if name != "user-events-python" {
			return runtime.PoolSnapshot{}, false
		}
		return runtime.PoolSnapshot{App: name, Capacity: 9, Containers: 5, Busy: 2, Idle: 3}, true
	}
	var outA, outB, errA, errB bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cmdA := New(logger, &outA, depsA)
	cmdA.Reader = strings.NewReader("")
	cmdA.ErrWriter = &errA
	cmdB := New(logger, &outB, depsB)
	cmdB.Reader = strings.NewReader("")
	cmdB.ErrWriter = &errB

	// Mutating the caller's values after New must not retarget either command.
	depsA.PoolSnapshotProvider = noPoolProvider
	depsB.PoolSnapshotProvider = depsA.PoolSnapshotProvider

	args := []string{"relay", "app", "inspect", "user-events-python"}
	if err := cmdA.Run(t.Context(), args); err != nil {
		t.Fatalf("tree A inspect: %v", err)
	}
	if err := cmdB.Run(t.Context(), args); err != nil {
		t.Fatalf("tree B inspect: %v", err)
	}
	if errA.Len() != 0 || errB.Len() != 0 {
		t.Fatalf("CLI command errors: tree A %q; tree B %q", errA.String(), errB.String())
	}

	for name, output := range map[string]string{"tree A": normWS(outA.String()), "tree B": normWS(outB.String())} {
		for _, row := range []string{"Warm acquires: 7", "Cold starts: 3", "Discarded: 2"} {
			if !strings.Contains(output, row) {
				t.Errorf("%s must use persisted counters, missing %q:\n%s", name, row, output)
			}
		}
		if strings.Contains(output, "unknown") {
			t.Errorf("%s should have its own live provider:\n%s", name, output)
		}
	}
	if !strings.Contains(outA.String(), "Capacity: 4") || strings.Contains(outA.String(), "Capacity: 9") {
		t.Errorf("tree A must render provider A only:\n%s", outA.String())
	}
	if !strings.Contains(outB.String(), "Capacity: 9") || strings.Contains(outB.String(), "Capacity: 4") {
		t.Errorf("tree B must render provider B only:\n%s", outB.String())
	}
	if callsA != 1 || callsB != 1 {
		t.Errorf("provider calls: A=%d B=%d; want one call per tree", callsA, callsB)
	}
}

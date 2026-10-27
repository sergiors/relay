package cli

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/worker"
)

// TestAppInspectRuntimePoolStartingRow verifies the Starting row appears
// only when an in-flight lazy start is observed.
func TestAppInspectRuntimePoolStartingRow(t *testing.T) {
	var w bytes.Buffer
	printRuntimePool(&w, &poolGauges{Capacity: 2, Starting: 1}, 0, 0, 0)
	out := normWS(w.String())
	if !strings.Contains(out, "Starting: 1") {
		t.Errorf("starting row missing:\n%s", out)
	}
}

// fakePoolSnapshotter is an in-memory worker.PoolSnapshotter for CLI tests, so
// the live query socket can be exercised without Docker or a runtime.Manager.
type fakePoolSnapshotter struct {
	pools map[string]runtime.PoolSnapshot
}

func (f fakePoolSnapshotter) PoolSnapshot(name string) (runtime.PoolSnapshot, bool) {
	s, ok := f.pools[name]
	return s, ok
}

// noPoolProvider is the in-process provider shape that answers every app with
// not-found: wired but unable to resolve a live pool (an embedding host or test
// exists, but this app's pool does not).
func noPoolProvider(string) (runtime.PoolSnapshot, bool) {
	return runtime.PoolSnapshot{}, false
}

// recordingPoolSnapshotter is a fakePoolSnapshotter that also records which app
// names it served, so resolution tests can pin WHICH tree dialed WHICH socket.
// Queries are answered on the socket server's goroutine; the mutex keeps the
// record race-free when the test reads it after the round trip returns.
type recordingPoolSnapshotter struct {
	mu      sync.Mutex
	pools   map[string]runtime.PoolSnapshot
	queries []string
}

func (f *recordingPoolSnapshotter) PoolSnapshot(name string) (runtime.PoolSnapshot, bool) {
	s, ok := f.pools[name]
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, name)
	return s, ok
}

// recorded drains the recorded query names without exposing internal state.
func (f *recordingPoolSnapshotter) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

// startUnitSocketServer starts a real in-process worker query socket at a
// fresh /tmp path (to stay inside the Unix socket sun_path limit) backed by the
// supplied snapshotter. The CLI's socket fallback is thereby exercised through
// the real wire protocol with no Docker, no runtime.Manager, and no database,
// so these resolution tests stay in the infrastructure-independent unit run
// (mirroring the integration helper startTestSocketServer). It returns the
// bound path.
func startUnitSocketServer(t *testing.T, snap worker.PoolSnapshotter) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "relay-cli-sock-")
	if err != nil {
		t.Fatalf("tmp socket dir: %v", err)
	}
	sockPath := filepath.Join(dir, "relay.sock")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	s, err := worker.NewSocketServer(
		sockPath,
		snap,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return sockPath
}

// pooledStats is the persisted per-app stats row passed to appendRuntimePool —
// the same struct appInspect carries in from the state database. The live
// sources in these tests deliberately report different counter values that must
// never render: the cumulative counters are persisted-state only.
func pooledStats() state.AppStats {
	return state.AppStats{
		App:               "user-events-python",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
	}
}

// TestAppendRuntimePoolProviderPrecedesSocket pins the first leg of the live
// gauge resolution order: when the injected in-process provider reports a pool
// for the app, its gauges render even though the worker socket also answers —
// the socket is never dialed, and the cumulative counters still come from the
// persisted stats row rather than from either live source.
func TestAppendRuntimePoolProviderPrecedesSocket(t *testing.T) {
	// The socket holds deliberately different gauges (and a different acquire
	// counter than either the provider or the persisted row).
	socket := &recordingPoolSnapshotter{pools: map[string]runtime.PoolSnapshot{
		"user-events-python": {App: "user-events-python", Capacity: 40, Containers: 20, Busy: 10, Idle: 10, WarmAcquires: 800},
	}}
	sockPath := startUnitSocketServer(t, socket)

	var called int
	prov := PoolSnapshotProvider(func(name string) (runtime.PoolSnapshot, bool) {
		called++
		if name != "user-events-python" {
			return runtime.PoolSnapshot{}, false
		}
		return runtime.PoolSnapshot{
			App:          name,
			Capacity:     4,
			Containers:   2,
			Busy:         1,
			Idle:         1,
			WarmAcquires: 700,
			ColdStarts:   300,
			Discarded:    200,
		}, true
	})

	var w bytes.Buffer
	appendRuntimePool(&w, "user-events-python", pooledStats(), prov, sockPath)
	out := normWS(w.String())
	// The provider's gauges — not the socket's 40/20/10/10.
	for _, row := range []string{"Capacity: 4", "Containers: 2", "Busy: 1", "Idle: 1"} {
		if !strings.Contains(out, row) {
			t.Errorf("provider-backed inspect missing %q:\n%s", row, out)
		}
	}
	if strings.Contains(out, "Capacity: 40") || strings.Contains(out, "unknown") {
		t.Errorf("the provider must win over the socket:\n%s", out)
	}
	// The provider takes precedence, so the socket is spared entirely.
	if got := socket.recorded(); len(got) != 0 {
		t.Errorf("socket must not be consulted while the provider resolves: %v", got)
	}
	if called != 1 {
		t.Errorf("provider called %d times, want exactly once", called)
	}
}

// TestAppendRuntimePoolSocketBacksUnwiredProvider pins the fallback leg: with
// the provider unwired (nil, the standalone CLI), the live gauges come from the
// worker query socket — through the real wire protocol — while the persisted
// counters still render.
func TestAppendRuntimePoolSocketBacksUnwiredProvider(t *testing.T) {
	socket := &recordingPoolSnapshotter{pools: map[string]runtime.PoolSnapshot{
		"user-events-python": {App: "user-events-python", Capacity: 40, Containers: 20, Busy: 10, Idle: 10},
	}}
	sockPath := startUnitSocketServer(t, socket)

	var w bytes.Buffer
	appendRuntimePool(&w, "user-events-python", pooledStats(), nil, sockPath)
	out := normWS(w.String())
	for _, row := range []string{"Capacity: 40", "Containers: 20", "Busy: 10", "Idle: 10"} {
		if !strings.Contains(out, row) {
			t.Errorf("socket-backed inspect (nil provider) missing %q:\n%s", row, out)
		}
	}
	if strings.Contains(out, "unknown") {
		t.Errorf("socket-backed inspect (nil provider) must not render unknown gauges:\n%s", out)
	}
}

// TestAppendRuntimePoolSocketBacksProviderWithoutPool pins that a WIRED
// provider that answers not-found does not short-circuit the fallback: the
// socket that does know the app still supplies the gauges.
func TestAppendRuntimePoolSocketBacksProviderWithoutPool(t *testing.T) {
	socket := &recordingPoolSnapshotter{pools: map[string]runtime.PoolSnapshot{
		"user-events-python": {App: "user-events-python", Capacity: 9, Containers: 5, Busy: 2, Idle: 3},
	}}
	sockPath := startUnitSocketServer(t, socket)

	var w bytes.Buffer
	appendRuntimePool(&w, "user-events-python", pooledStats(), noPoolProvider, sockPath)
	out := normWS(w.String())
	for _, row := range []string{"Capacity: 9", "Containers: 5", "Busy: 2", "Idle: 3"} {
		if !strings.Contains(out, row) {
			t.Errorf("socket-backed inspect (not-found provider) missing %q:\n%s", row, out)
		}
	}
	if strings.Contains(out, "unknown") {
		t.Errorf("not-found provider must fall through to the live socket:\n%s", out)
	}
}

// TestAppendRuntimePoolUnknownWhenNoLiveSource pins the unavailable leg: with
// the provider nil and no worker running at the socket path, the gauges render
// as explicitly unknown and the cumulative counters STILL come from the
// persisted stats row — never from a stale live guess.
func TestAppendRuntimePoolUnknownWhenNoLiveSource(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "relay-cli-sock-")
	if err != nil {
		t.Fatalf("tmp socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	// The directory exists but nothing serves the socket: the dial fails.

	var w bytes.Buffer
	appendRuntimePool(&w, "user-events-python", pooledStats(), nil, filepath.Join(dir, "unused.sock"))
	out := normWS(w.String())
	for _, row := range []string{
		"Capacity: unknown",
		"Containers: unknown",
		"Busy: unknown",
		"Idle: unknown",
		// The persisted counters are the ONLY numbers this shape renders.
		"Warm acquires: 7",
		"Cold starts: 3",
		"Discarded: 2",
	} {
		if !strings.Contains(out, row) {
			t.Errorf("unavailable inspect missing %q:\n%s", row, out)
		}
	}
}

// TestPoolProviderClosuresDoNotCrossContaminate pins the command-local pool
// contract at the unit level: two independently constructed Dependencies trees
// each carry their OWN provider closure and socket path, and the same
// resolution chain appInspect drives (appendRuntimePool with the tree's deps
// fields) resolves every tree from its own sources only. A regression that
// captures a shared provider, leaks a closure into package state, cross-wires
// socket paths, or lets one tree read the other's pool shows up here as the
// wrong gauges in one tree's output — every source below reports deliberately
// distinct values.
func TestPoolProviderClosuresDoNotCrossContaminate(t *testing.T) {
	const sharedName = "user-events-python"

	// Tree A: its in-process provider knows the app; its socket holds values no
	// tree must render (the provider answers first), and it never sees B's
	// traffic.
	socketA := &recordingPoolSnapshotter{pools: map[string]runtime.PoolSnapshot{
		sharedName: {App: sharedName, Capacity: 40, Containers: 20, Busy: 10, Idle: 10},
	}}
	var callsA int
	depsA := Dependencies{
		PoolSnapshotProvider: func(name string) (runtime.PoolSnapshot, bool) {
			if name != sharedName {
				return runtime.PoolSnapshot{}, false
			}
			callsA++
			return runtime.PoolSnapshot{
				App:          sharedName,
				Capacity:     4,
				Containers:   2,
				Busy:         1,
				Idle:         1,
				WarmAcquires: 700,
				ColdStarts:   300,
				Discarded:    200,
			}, true
		},
		SocketPath: startUnitSocketServer(t, socketA),
	}

	// Tree B's provider answers not-found for everything (its worker pool has
	// not warmed this app), so the resolution must fall back to B's OWN socket,
	// whose gauges differ from A's provider's and from A's socket's.
	socketB := &recordingPoolSnapshotter{pools: map[string]runtime.PoolSnapshot{
		sharedName: {App: sharedName, Capacity: 9, Containers: 5, Busy: 2, Idle: 3},
	}}
	var callsB int
	depsB := Dependencies{
		PoolSnapshotProvider: func(string) (runtime.PoolSnapshot, bool) {
			callsB++
			return runtime.PoolSnapshot{}, false
		},
		SocketPath: startUnitSocketServer(t, socketB),
	}

	// The same persisted stats row (the part of the section appInspect reads
	// from the state database) feeds both trees.
	row := pooledStats()

	var outA, outB bytes.Buffer
	appendRuntimePool(&outA, sharedName, row, depsA.PoolSnapshotProvider, depsA.SocketPath)
	appendRuntimePool(&outB, sharedName, row, depsB.PoolSnapshotProvider, depsB.SocketPath)

	a, b := normWS(outA.String()), normWS(outB.String())

	// Tree A renders ITS provider's gauges.
	for _, want := range []string{"Runtime pool:", "Capacity: 4", "Containers: 2", "Busy: 1", "Idle: 1"} {
		if !strings.Contains(a, want) {
			t.Errorf("tree A missing %q:\n%s", want, a)
		}
	}
	// Tree B renders ITS OWN socket's gauges (its provider reported no pool)
	// — no reading from A's provider closure or A's socket.
	for _, want := range []string{"Runtime pool:", "Capacity: 9", "Containers: 5", "Busy: 2", "Idle: 3"} {
		if !strings.Contains(b, want) {
			t.Errorf("tree B missing %q:\n%s", want, b)
		}
	}
	// Neither tree rendered the other's gauges.
	if strings.Contains(b, "Capacity: 4") || strings.Contains(b, "Capacity: 40") {
		t.Errorf("tree B saw tree A's pool:\n%s", b)
	}
	if strings.Contains(a, "Capacity: 9") || strings.Contains(a, "unknown") {
		t.Errorf("tree A did not resolve its own provider's pool:\n%s", a)
	}

	// Each tree consulted only its own closure, exactly once.
	if callsA != 1 {
		t.Errorf("provider A invoked %d times, want exactly once by tree A", callsA)
	}
	if callsB != 1 {
		t.Errorf("provider B invoked %d times, want exactly once by tree B", callsB)
	}

	// The provider/socket wiring holds per tree: B fell back to its socket
	// (which recorded exactly its one query), while A's socket was never
	// dialed (the provider answers first).
	if got := socketA.recorded(); len(got) != 0 {
		t.Errorf("tree A socket must not be dialed while its provider resolves: %v", got)
	}
	if got := socketB.recorded(); len(got) != 1 || got[0] != sharedName {
		t.Errorf("tree B socket queries = %v, want exactly [%s]", got, sharedName)
	}
	// The persisted counters are identical in both trees and are the row's
	// values — no live counter (700/300/200) ever renders.
	for _, out := range []string{a, b} {
		for _, want := range []string{"Warm acquires: 7", "Cold starts: 3", "Discarded: 2"} {
			if !strings.Contains(out, want) {
				t.Errorf("persisted counters missing %q during cross-tree run:\n%s", want, out)
			}
		}
		if strings.Contains(out, "Warm acquires: 700") || strings.Contains(out, "Cold starts: 300") {
			t.Errorf("live source counter leaked into the rendered section:\n%s", out)
		}
	}
}

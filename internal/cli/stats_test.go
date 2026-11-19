//go:build integration

package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
	"relay/internal/worker"
)

// statsObservedAtMs is the seeded backlog observation instant: the aggregate
// only surfaces gauge pairs that carry a successful-observation timestamp, so
// seeded gauges must carry one too.
const statsObservedAtMs = int64(1767225600000)

// fakeStatsResetter counts the node-scoped and global socket reset commands
// separately, so a CLI test can prove WHICH command reached the worker. Its
// nodeID is the identity the socket's local reset verifies against the CLI's
// NODE_ID before resetting anything.
type fakeStatsResetter struct {
	mu         sync.Mutex
	nodeID     string
	localCalls int
	allCalls   int
	err        error
}

var _ worker.NodeStatsResetter = (*fakeStatsResetter)(nil)
var _ worker.GlobalStatsResetter = (*fakeStatsResetter)(nil)

func (f *fakeStatsResetter) ResetStats() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localCalls++
	return f.err
}

func (f *fakeStatsResetter) ResetAllStats() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allCalls++
	return f.err
}

func (f *fakeStatsResetter) NodeID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nodeID
}

func (f *fakeStatsResetter) counts() (local, all int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.localCalls, f.allCalls
}

// localNodeID returns the NODE_ID the seed fixtures set for this test — the
// identity a local-socket resetter must echo back to pass the worker's
// expected-target check. It fails loudly when the fixture did not seed one,
// rather than silently exercising the empty-target rejection instead.
func localNodeID(t *testing.T) string {
	t.Helper()
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		t.Fatal("NODE_ID not set; seed the stats state before starting a local socket")
	}
	return nodeID
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

// statsLocalNode returns the per-test node identity `relay stats` reads by
// default (NODE_ID). It is unique per test because a node-identity handle takes
// a PostgreSQL session advisory lock, which is database-wide — per-test schemas
// do not namespace it, and other packages' tests share the server.
func statsLocalNode(t *testing.T) string {
	t.Helper()
	return testutil.UniqueName(t, "stats-local")
}

// recordNodeStats writes one node's snapshot through a node-scoped handle —
// exactly the way that node's worker flushes it, since stats rows are keyed by
// node identity — and closes the handle immediately so the per-node lock is
// released for later handles.
func recordNodeStats(t *testing.T, dsn, nodeID string, stats state.Stats, apps ...state.AppStats) {
	t.Helper()
	st, err := state.Open(context.Background(), state.Options{
		DSN:      dsn,
		NodeID:   nodeID,
		Instance: "inst-" + nodeID,
	})
	if err != nil {
		t.Fatalf("open node state (%s): %v", nodeID, err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Errorf("close node state (%s): %v", nodeID, err)
		}
	}()
	st.RecordStats(stats)
	for _, app := range apps {
		st.RecordAppStats(app)
	}
}

// seedNodeStatsState creates a temp state DB, seeds nodeID's stats row (and
// any app rows) as that node's worker would, and points NODE_ID at nodeID so
// the default `relay stats` / `relay stats reset` select this node. It returns
// an identity-free admin handle for assertions, the deps whose OpenState reads
// the same schema, and the DSN so a test can seed further nodes.
func seedNodeStatsState(
	t *testing.T, nodeID string, stats state.Stats, apps ...state.AppStats,
) (*state.State, Dependencies, string) {
	t.Helper()
	t.Setenv("NODE_ID", nodeID)
	deps, dsn := stateTestDepsWithDSN(t)
	recordNodeStats(t, dsn, nodeID, stats, apps...)

	st, cleanup, err := deps.OpenState(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(cleanup)
	return st, deps, dsn
}

// seedStatsState seeds the DEFAULT local node with a known snapshot: the
// cumulative counters, the backlog gauges, and their observation instant (which
// is what lets an aggregate read surface the gauges at all).
func seedStatsState(t *testing.T, apps ...state.AppStats) (*state.State, Dependencies, string) {
	t.Helper()
	return seedNodeStatsState(t, statsLocalNode(t), state.Stats{
		EventsReceivedTotal:     153000,
		EventsMatchedTotal:      152934,
		EventsUnmatchedTotal:    66,
		HandlerSuccessTotal:     152801,
		HandlerFailureTotal:     133,
		RetryTotal:              82,
		DLQTotal:                4,
		PendingEntries:          17,
		OldestPendingAgeSeconds: 134,
		PendingObservedAtUnixMs: statsObservedAtMs,
	}, apps...)
}

// renderStats renders the recorded snapshot with tab-aligned labels, Go-duration
// age formatting, and a relative Updated timestamp.
func TestPrintStats(t *testing.T) {
	st, _, _ := seedStatsState(t)
	var w bytes.Buffer
	renderStatsForTest(t, &w, st)
	out := w.String()

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 10 {
		t.Fatalf("expected 10 lines, got %d:\n%s", len(lines), out)
	}

	// "Oldest pending age:" and "Events unmatched:" are among the longest
	// labels, so tabwriter pads values to a common column; assert the exact
	// aligned rows.
	wants := []string{
		"Events received:     153000",
		"Events matched:      152934",
		"Events unmatched:    66",
		"Handler successes:   152801",
		"Handler failures:    133",
		"Message reclaims:    82",
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
	if !strings.Contains(lines[9], "Updated:") || !strings.Contains(lines[9], "ago") {
		t.Errorf("row 9 want Updated + relative age, got %q", lines[9])
	}
}

// A fresh state DB has no stats row; renderStats renders all zeros, "0s" age,
// and "never" for Updated, and the command exits 0.
func TestPrintStatsEmptyDB(t *testing.T) {
	t.Setenv("NODE_ID", testutil.UniqueName(t, "stats-empty"))
	st, deps := openTempState(t)

	var w bytes.Buffer
	renderStatsForTest(t, &w, st)
	out := w.String()
	for _, want := range []string{
		"Events received:     0",
		"Handler successes:   0",
		"Handler failures:    0",
		"Message reclaims:    0",
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
	_, deps, _ := seedStatsState(t)
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

// The default read is NODE_ID's row ONLY — never an implicit aggregate — and
// `--node` reads the named node's row without consulting NODE_ID at all.
func TestStatsDefaultReadsOnlyLocalNode(t *testing.T) {
	local := statsLocalNode(t)
	remote := testutil.UniqueName(t, "stats-remote")
	_, deps, dsn := seedNodeStatsState(t, local, state.Stats{
		EventsReceivedTotal: 11,
		HandlerSuccessTotal: 2,
	})
	recordNodeStats(t, dsn, remote, state.Stats{
		EventsReceivedTotal: 4700,
		HandlerSuccessTotal: 88,
	})

	out, _, err := runCLIWithDeps(t, deps, "", "stats")
	if err != nil {
		t.Fatalf("stats: err = %v, want nil", err)
	}
	flat := normWS(out)
	if !strings.Contains(flat, "Events received: 11") {
		t.Fatalf("default stats missing NODE_ID's counters:\n%s", out)
	}
	if strings.Contains(flat, "4700") || strings.Contains(flat, "88") {
		t.Fatalf("default stats must not aggregate other nodes:\n%s", out)
	}

	// --node selects the named row and needs no NODE_ID whatsoever.
	t.Setenv("NODE_ID", "")
	out, _, err = runCLIWithDeps(t, deps, "", "stats", "--node", remote)
	if err != nil {
		t.Fatalf("stats --node: err = %v, want nil", err)
	}
	flat = normWS(out)
	if !strings.Contains(flat, "Events received: 4700") {
		t.Fatalf("stats --node missing the selected node's counters:\n%s", out)
	}
	if strings.Contains(flat, "Events received: 11") {
		t.Fatalf("stats --node must read only the named node:\n%s", out)
	}
}

// `relay stats --all` needs no NODE_ID: it sums the cumulative counters across
// every node and presents ONE backlog gauge pair — the freshest complete
// observation — while a legacy row without an observation instant never
// supplies aggregate gauges however large its values are.
func TestStatsAllAggregatesAcrossNodes(t *testing.T) {
	local := statsLocalNode(t)
	remote := testutil.UniqueName(t, "stats-remote")
	_, deps, dsn := seedNodeStatsState(t, local, state.Stats{
		EventsReceivedTotal: 11,
		// Legacy gauges: no observation instant, so they may never surface.
		PendingEntries:          500,
		OldestPendingAgeSeconds: 9999,
	})
	recordNodeStats(t, dsn, remote, state.Stats{
		EventsReceivedTotal:     4700,
		PendingEntries:          4,
		OldestPendingAgeSeconds: 5,
		PendingObservedAtUnixMs: statsObservedAtMs,
	})
	t.Setenv("NODE_ID", "")

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "--all")
	if err != nil {
		t.Fatalf("stats --all: err = %v, want nil", err)
	}
	flat := normWS(out)
	if !strings.Contains(flat, "Events received: 4711") {
		t.Fatalf("--all must sum counters across nodes (11 + 4700):\n%s", out)
	}
	if !strings.Contains(flat, "Pending entries: 4") ||
		!strings.Contains(flat, "Oldest pending age: 5s") {
		t.Fatalf("--all must show the single observed backlog pair:\n%s", out)
	}
	if strings.Contains(flat, "500") || strings.Contains(flat, "9999s") {
		t.Fatalf("--all must not surface legacy (unobserved) gauges:\n%s", out)
	}
}

// `relay stats reset` with no running worker falls back to the state database:
// it prints the exact confirmation line, zeroes NODE_ID's cumulative counters
// and every per-app row IN PLACE, but PRESERVES the live backlog gauges and
// their observation instant. A following bare `relay stats` renders zeros for
// the counters and the preserved gauge value.
func TestStatsResetCommand(t *testing.T) {
	exec := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	st, deps, _ := seedStatsState(t, state.AppStats{
		App:                "alpha",
		EventsMatchedTotal: 9,
		WarmAcquiresTotal:  3,
		LastExecutionAt:    exec,
	})

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err != nil {
		t.Fatalf("stats reset: err = %v, want nil", err)
	}
	want := "Stats reset for node " + localNodeID(t) + "\n"
	if out != want {
		t.Fatalf("stats reset stdout = %q, want %q", out, want)
	}

	// Persisted state: counters zeroed, gauges and their observation preserved,
	// app rows reset in place (not deleted).
	s, ok := st.Stats()
	if !ok {
		t.Fatal("stats row must survive the reset")
	}
	if s.EventsReceivedTotal != 0 || s.EventsMatchedTotal != 0 || s.EventsUnmatchedTotal != 0 ||
		s.HandlerSuccessTotal != 0 ||
		s.HandlerFailureTotal != 0 || s.RetryTotal != 0 || s.DLQTotal != 0 {
		t.Fatalf("cumulative counters must be zeroed: %+v", s)
	}
	if s.PendingEntries != 17 || s.OldestPendingAgeSeconds != 134 ||
		s.PendingObservedAtUnixMs != statsObservedAtMs {
		t.Fatalf("backlog gauges and observation must be preserved: %+v", s)
	}
	all := st.AllAppStats()
	if len(all) != 1 || all[0].App != "alpha" {
		t.Fatalf("app_stats rows must be preserved: %+v", all)
	}
	if all[0].EventsMatchedTotal != 0 || all[0].WarmAcquiresTotal != 0 || all[0].LastExecutionAt != "" {
		t.Fatalf("app_stats must be zeroed in place: %+v", all[0])
	}

	// A follow-up bare `relay stats` still renders the table (the parent Action
	// with no subcommand token), now with zero counters and the preserved gauge.
	rendered, _, err := runCLIWithDeps(t, deps, "", "stats")
	if err != nil {
		t.Fatalf("stats after reset: err = %v, want nil", err)
	}
	for _, want := range []string{
		"Events received:     0",
		"Handler successes:   0",
		"Handler failures:    0",
		"Message reclaims:    0",
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

// A bare `relay stats reset` is NODE-SCOPED: it zeroes the local node's global
// and per-app rows (counters and execution history) while the other node's
// rows, gauges, and app history survive untouched.
func TestStatsResetCommandResetsOnlyLocalNode(t *testing.T) {
	local := statsLocalNode(t)
	remote := testutil.UniqueName(t, "stats-remote")
	newerExec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	olderExec := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)

	st, deps, dsn := seedNodeStatsState(t, local,
		state.Stats{
			EventsReceivedTotal:     11,
			PendingEntries:          17,
			OldestPendingAgeSeconds: 134,
			PendingObservedAtUnixMs: statsObservedAtMs,
		},
		state.AppStats{App: "alpha", EventsMatchedTotal: 9, LastExecutionAt: newerExec})
	recordNodeStats(t, dsn, remote,
		state.Stats{EventsReceivedTotal: 4700},
		state.AppStats{App: "alpha", EventsMatchedTotal: 4, LastExecutionAt: olderExec})

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err != nil {
		t.Fatalf("stats reset: err = %v, want nil", err)
	}
	want := "Stats reset for node " + local + "\n"
	if out != want {
		t.Fatalf("stats reset stdout = %q, want %q", out, want)
	}

	// The local node's counters zero while its gauges survive the reset.
	localOut, _, err := runCLIWithDeps(t, deps, "", "stats", "--node", local)
	if err != nil {
		t.Fatalf("stats --node local: err = %v, want nil", err)
	}
	for _, want := range []string{"Events received: 0", "Pending entries: 17", "Oldest pending age: 2m14s"} {
		if !strings.Contains(normWS(localOut), want) {
			t.Errorf("local node after reset missing %q:\n%s", want, localOut)
		}
	}

	// The remote node is untouched.
	remoteOut, _, err := runCLIWithDeps(t, deps, "", "stats", "--node", remote)
	if err != nil {
		t.Fatalf("stats --node remote: err = %v, want nil", err)
	}
	if !strings.Contains(normWS(remoteOut), "Events received: 4700") {
		t.Fatalf("remote node must survive the local reset:\n%s", remoteOut)
	}

	// Per-app history is scoped the same way: the local row zeroed (its NEWER
	// execution timestamp cleared), the remote row preserved verbatim — the
	// aggregate therefore carries only the remote's older timestamp.
	all := st.AllAppStats()
	if len(all) != 1 || all[0].App != "alpha" {
		t.Fatalf("app_stats rows after reset = %+v, want one alpha row", all)
	}
	if all[0].EventsMatchedTotal != 4 {
		t.Fatalf("alpha events after local reset = %d, want 4 (remote only)", all[0].EventsMatchedTotal)
	}
	if all[0].LastExecutionAt != olderExec {
		t.Fatalf("alpha execution = %q, want the remote's %q (local's newer one cleared)",
			all[0].LastExecutionAt, olderExec)
	}
}

// `relay stats reset --all` needs no NODE_ID: it zeroes EVERY node's counters
// and per-app rows through the global reset, preserving backlog gauges, while
// the freshest observed pair still wins the aggregate read afterwards.
func TestStatsResetAllWithoutNodeIDResetsEveryNode(t *testing.T) {
	local := statsLocalNode(t)
	remote := testutil.UniqueName(t, "stats-remote")
	exec := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)

	st, deps, dsn := seedNodeStatsState(t, local,
		state.Stats{
			EventsReceivedTotal:     11,
			PendingEntries:          17,
			OldestPendingAgeSeconds: 134,
			PendingObservedAtUnixMs: statsObservedAtMs,
		},
		state.AppStats{App: "alpha", EventsMatchedTotal: 9, LastExecutionAt: exec})
	recordNodeStats(t, dsn, remote,
		state.Stats{
			EventsReceivedTotal:     4700,
			PendingEntries:          9,
			OldestPendingAgeSeconds: 8,
			PendingObservedAtUnixMs: statsObservedAtMs - 1,
		},
		state.AppStats{App: "alpha", EventsMatchedTotal: 4, LastExecutionAt: exec})
	t.Setenv("NODE_ID", "")

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset", "--all")
	if err != nil {
		t.Fatalf("stats reset --all: err = %v, want nil", err)
	}
	if out != "Stats reset for all nodes\n" {
		t.Fatalf("stats reset --all stdout = %q, want %q", out, "Stats reset for all nodes\n")
	}

	// Every node's counters are zero; the local node's fresher observed pair is
	// still what the aggregate presents.
	aggOut, _, err := runCLIWithDeps(t, deps, "", "stats", "--all")
	if err != nil {
		t.Fatalf("stats --all after reset: err = %v, want nil", err)
	}
	flat := normWS(aggOut)
	if !strings.Contains(flat, "Events received: 0") {
		t.Fatalf("--all after reset must show zero counters:\n%s", aggOut)
	}
	if !strings.Contains(flat, "Pending entries: 17") ||
		!strings.Contains(flat, "Oldest pending age: 2m14s") {
		t.Fatalf("--all after reset must preserve the freshest observed pair:\n%s", aggOut)
	}

	// Both nodes' per-app rows were zeroed in place, timestamps included.
	all := st.AllAppStats()
	if len(all) != 1 || all[0].App != "alpha" {
		t.Fatalf("app_stats rows after --all reset = %+v, want one alpha row", all)
	}
	if all[0].EventsMatchedTotal != 0 || all[0].LastExecutionAt != "" {
		t.Fatalf("alpha after --all reset = %+v, want zeroed counters and cleared timestamps", all[0])
	}
}

// `relay stats reset` surfaces a worker-reported reset failure instead of
// silently falling back to the direct state write (which would mask it). The
// resetter names the CLI's NODE_ID, so the failure reaching the operator is the
// reset itself — not the target check.
func TestStatsResetCommandWorkerFailureSurfaces(t *testing.T) {
	st, deps, _ := seedStatsState(t)
	resetter := &fakeStatsResetter{
		nodeID: localNodeID(t),
		err:    errors.New("state unavailable"),
	}
	startStatsSocket(t, deps.SocketPath, resetter)

	_, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err == nil || !strings.Contains(err.Error(), "stats reset") {
		t.Fatalf("worker failure must surface as an error, got %v", err)
	}
	if local, all := resetter.counts(); local != 1 || all != 0 {
		t.Fatalf("socket reset commands = local %d, all %d; want 1/0 (the worker was asked and failed)", local, all)
	}
	// The CLI must not have written the state DB as a silent fallback.
	s, _ := st.Stats()
	if s.EventsReceivedTotal != 153000 {
		t.Fatalf("failed worker reset must not fall back to a state write: %+v", s)
	}
}

// `relay stats reset` with a RUNNING worker resets through the worker's socket
// — via the node-scoped command naming the CLI's NODE_ID — so the worker's
// in-memory totals are reset too, and the CLI does NOT fall back to the direct
// state write. The state DB is left untouched by the CLI itself (the worker
// owns the persisted reset).
func TestStatsResetCommandRunningWorker(t *testing.T) {
	st, deps, _ := seedStatsState(t)
	// A worker-owned state DB is a separate handle; the CLI's direct fallback
	// would write here, so an untouched row proves the socket path was taken.
	// The resetter reports the SAME node identity as the CLI's NODE_ID, so the
	// worker's expected-target check passes.
	resetter := &fakeStatsResetter{nodeID: localNodeID(t)}
	startStatsSocket(t, deps.SocketPath, resetter)

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err != nil {
		t.Fatalf("stats reset: err = %v, want nil", err)
	}
	want := "Stats reset for node " + localNodeID(t) + "\n"
	if out != want {
		t.Fatalf("stats reset stdout = %q, want %q", out, want)
	}
	local, all := resetter.counts()
	if local != 1 || all != 0 {
		t.Fatalf("socket reset commands = local %d, all %d; want 1/0 (the local command)", local, all)
	}
	// The CLI must not have written the state DB directly.
	s, _ := st.Stats()
	if s.EventsReceivedTotal != 153000 {
		t.Fatalf("CLI must not reset the state DB when a worker answered: %+v", s)
	}
}

// A running worker whose NODE_ID differs from the CLI's rejects the local reset
// BEFORE resetting anything (a drifted CLI environment must not clear another
// node's counters): the command fails with the worker's mismatch report, the
// resetter is never invoked, and there is NO direct state-DB fallback — the
// persisted row keeps its counters.
func TestStatsResetCommandNodeMismatchNoFallback(t *testing.T) {
	st, deps, _ := seedStatsState(t)
	resetter := &fakeStatsResetter{nodeID: testutil.UniqueName(t, "stats-other-worker")}
	startStatsSocket(t, deps.SocketPath, resetter)

	_, _, err := runCLIWithDeps(t, deps, "", "stats", "reset")
	if err == nil || !strings.Contains(err.Error(), "stats reset") {
		t.Fatalf("node mismatch must surface as an error, got %v", err)
	}
	if !strings.Contains(err.Error(), "node id mismatch") {
		t.Fatalf("error must carry the worker's mismatch report, got %v", err)
	}
	if local, all := resetter.counts(); local != 0 || all != 0 {
		t.Fatalf("socket reset commands = local %d, all %d; want 0/0 (rejected before resetting)",
			local, all)
	}
	// The mismatch is a worker-reported failure, not an absent worker: the CLI
	// must not fall back to a direct write behind the live worker's back.
	s, _ := st.Stats()
	if s.EventsReceivedTotal != 153000 {
		t.Fatalf("node mismatch must not fall back to a state write: %+v", s)
	}
}

// `relay stats reset --all` with a RUNNING worker goes through the socket's
// GLOBAL reset command — never the node-scoped one, and with no expected node
// identity — and, like the local form, never falls back to a direct state write
// behind the worker's back.
func TestStatsResetAllCommandRunningWorker(t *testing.T) {
	st, deps, _ := seedStatsState(t)
	resetter := &fakeStatsResetter{nodeID: localNodeID(t)}
	startStatsSocket(t, deps.SocketPath, resetter)

	out, _, err := runCLIWithDeps(t, deps, "", "stats", "reset", "--all")
	if err != nil {
		t.Fatalf("stats reset --all: err = %v, want nil", err)
	}
	if out != "Stats reset for all nodes\n" {
		t.Fatalf("stats reset --all stdout = %q, want %q", out, "Stats reset for all nodes\n")
	}
	local, all := resetter.counts()
	if local != 0 || all != 1 {
		t.Fatalf("socket reset commands = local %d, all %d; want 0/1 (the global command)", local, all)
	}
	s, _ := st.Stats()
	if s.EventsReceivedTotal != 153000 {
		t.Fatalf("CLI must not reset the state DB when a worker answered: %+v", s)
	}
}

// `relay stats reset` must NOT fall back to a direct state-DB write when a live
// socket accepts but never answers: the request is ambiguous (the worker may
// have received, or may still process, the reset), so the CLI surfaces the
// error and leaves the persisted totals alone. This preserves the live worker
// as the authority and avoids a second, racing reset.
func TestStatsResetCommandAmbiguousTimeoutNoFallback(t *testing.T) {
	st, deps, _ := seedStatsState(t)

	// A live listener that accepts and then hangs, so the client's read blocks
	// past its request deadline. The CLI must classify this as unavailable and
	// NOT write the state DB directly.
	ln, err := net.Listen("unix", deps.SocketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-release
	}()
	t.Cleanup(func() {
		close(release)
		_ = ln.Close()
		wg.Wait()
	})

	_, _, err = runCLIWithDeps(t, deps, "", "stats", "reset")
	if err == nil || !strings.Contains(err.Error(), "stats reset") {
		t.Fatalf("ambiguous timeout must surface as an error, got %v", err)
	}
	// The CLI must not have reset the state DB directly.
	s, _ := st.Stats()
	if s.EventsReceivedTotal != 153000 {
		t.Fatalf("ambiguous timeout must not trigger a direct state write: %+v", s)
	}
}

// `relay stats reset extra` is a usage error exiting 2, and the rejection
// happens before any state write.
func TestStatsResetTooManyArgs(t *testing.T) {
	st, deps, _ := seedStatsState(t)

	_, _, err := runCLIWithDeps(t, deps, "", "stats", "reset", "extra")
	if err == nil || !strings.Contains(err.Error(), "stats reset: too many arguments") {
		t.Fatalf("stats reset extra: missing rejection error: %v", err)
	}
	s, ok := st.Stats()
	if !ok || s.EventsReceivedTotal != 153000 {
		t.Fatalf("rejected reset must not write: %+v, ok=%v", s, ok)
	}
}

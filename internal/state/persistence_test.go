package state

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFunctionsDir creates a real functions tree (root/demo with a template and
// source) so RebuildFromFS has something to scan. It mirrors the setup in
// TestRebuildFromFSOnEmptyDB.
func writeFunctionsDir(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(twoHandlerTmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte("def handler(e): return e\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
}

// TestRestartPreservesGlobalStats is the core regression guard for the worker
// restart bug: a fresh process-lifetime metrics registry was snapshotted into
// the DB immediately, zeroing the persisted cumulative counters. Reopening the
// same DB (simulating a restart) and running the normal startup path must leave
// every cumulative counter at its exact persisted value.
func TestRestartPreservesGlobalStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	want := Stats{
		EventsMatchedTotal:      100,
		HandlerSuccessTotal:     70,
		HandlerFailureTotal:     30,
		RetryTotal:              5,
		DLQTotal:                2,
		PendingEntries:          9,
		OldestPendingAgeSeconds: 42,
	}
	c1.RecordStats(want)
	_ = c1.Close()

	// Reopen the same path: this is the worker restart, including idempotent
	// schema init.
	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	// Run the normal startup path pieces that touch the DB.
	root := t.TempDir()
	writeFunctionsDir(t, root)
	if err := c2.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	c2.RecordDiscovered(fnFor(t, "demo", mustTemplate(t, twoHandlerTmpl)))

	got, ok := c2.Stats()
	if !ok {
		t.Fatal("expected stats row after restart")
	}
	// Compare all fields except updated_at, which is rewritten on each record.
	want.UpdatedAt = got.UpdatedAt
	if got != want {
		t.Fatalf("stats after restart = %+v, want %+v", got, want)
	}
}

// TestRestartPreservesFunctionStats guards the per-function counterpart of the
// restart bug: reopening the DB and rediscovering an already-known function must
// not reset its persisted per-function counters.
func TestRestartPreservesFunctionStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	alpha := FunctionStats{Function: "alpha", EventsMatchedTotal: 10, HandlerSuccessTotal: 8, HandlerFailureTotal: 2, RetryTotal: 1, DLQTotal: 0}
	beta := FunctionStats{Function: "beta", EventsMatchedTotal: 20, HandlerSuccessTotal: 15, HandlerFailureTotal: 5, RetryTotal: 3, DLQTotal: 1}
	c1.RecordFunctionStats(alpha)
	c1.RecordFunctionStats(beta)
	_ = c1.Close()

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	root := t.TempDir()
	writeFunctionsDir(t, root)
	if err := c2.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	// Rediscover an already-known function; must not touch its stats row.
	c2.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	a, ok := c2.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats after restart")
	}
	alpha.UpdatedAt = a.UpdatedAt
	if a != alpha {
		t.Fatalf("alpha stats after restart = %+v, want %+v", a, alpha)
	}

	b, ok := c2.FunctionStats("beta")
	if !ok {
		t.Fatal("expected beta stats after restart")
	}
	beta.UpdatedAt = b.UpdatedAt
	if b != beta {
		t.Fatalf("beta stats after restart = %+v, want %+v", b, beta)
	}

	all := c2.AllFunctionStats()
	if len(all) != 2 {
		t.Fatalf("AllFunctionStats len = %d, want 2: %+v", len(all), all)
	}
	if all[0].Function != "alpha" || all[1].Function != "beta" {
		t.Fatalf("AllFunctionStats not in name order: %+v", all)
	}
	all[0].UpdatedAt = alpha.UpdatedAt
	all[1].UpdatedAt = beta.UpdatedAt
	if all[0] != alpha || all[1] != beta {
		t.Fatalf("AllFunctionStats = %+v, want %+v and %+v", all, alpha, beta)
	}
}

// TestRebuildFromFSOnNonEmptyDBKeepsFunctionStats is a belt-and-braces guard on
// the discovery path: RebuildFromFS on a DB whose functions table is already
// populated must not touch the function_stats rows.
func TestRebuildFromFSOnNonEmptyDBKeepsFunctionStats(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordDiscovered(fnFor(t, "demo", tmpl)) // populate the functions table
	c.RecordFunctionStats(FunctionStats{Function: "demo", EventsMatchedTotal: 7, HandlerSuccessTotal: 5})

	root := t.TempDir()
	writeFunctionsDir(t, root)
	if err := c.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	s, ok := c.FunctionStats("demo")
	if !ok {
		t.Fatal("expected function stats row after rebuild")
	}
	if s.EventsMatchedTotal != 7 || s.HandlerSuccessTotal != 5 {
		t.Fatalf("function stats changed by rebuild: %+v", s)
	}
}

// TestRestartResetsBuildingStatusToPreparing guards the restart boundary: a
// "building" status is an in-flight marker for a build that only this process
// was driving. If the worker dies mid-build, that status persists; on restart
// the startup discovery (RecordDiscovered) must re-seed the function as
// preparing rather than leaving a stale building state that would never clear.
// The same reset must cover stale ready/reconciling values from the crashed
// process. The status is asserted after reopening the same DB (a simulated
// restart), so the guard covers the persisted value, not just an in-memory
// write.
func TestRestartResetsBuildingStatusToPreparing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "demo", tmpl)
	c1.RecordDiscovered(fn)
	c1.RecordReconcileBuilding("demo")
	if got, _ := c1.GetFunction("demo"); got.Status != StatusBuilding {
		t.Fatalf("pre-restart status = %q, want building", got.Status)
	}
	_ = c1.Close()

	// Reopen the same path (the worker restart) and run the normal startup
	// discovery. The stale building marker must be reset to preparing.
	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	if got, _ := c2.GetFunction("demo"); got.Status != StatusBuilding {
		t.Fatalf("status after reopen = %q, want the persisted building status", got.Status)
	}
	c2.RecordDiscovered(fn)
	got, ok := c2.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo function after restart discovery")
	}
	if got.Status != StatusPreparing {
		t.Fatalf("status after rediscovery = %q, want preparing", got.Status)
	}
}

// TestRecordDiscoveredDoesNotResetFunctionStats is a unit-level guard: recording
// a function's discovery must not reset its persisted per-function counters or
// updated_at.
func TestRecordDiscoveredDoesNotResetFunctionStats(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsMatchedTotal: 10, HandlerSuccessTotal: 8, HandlerFailureTotal: 2, RetryTotal: 1, DLQTotal: 0})

	before, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats before discovery")
	}

	c.RecordDiscovered(fnFor(t, "alpha", tmpl))

	after, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats after discovery")
	}
	if after != before {
		t.Fatalf("function stats changed by discovery: before=%+v after=%+v", before, after)
	}
}

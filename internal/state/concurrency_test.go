package state

import (
	"bytes"
	"context"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

// captureLogger returns a *State whose logger writes into a bytes.Buffer, so a
// test can assert that no SQLITE_BUSY / "database is locked" error surfaced
// during concurrent access. With SetMaxOpenConns(1) the pool serializes all
// in-process access, so these errors must never appear.
func captureLogger(t *testing.T) (*State, *bytes.Buffer) {
	t.Helper()
	c, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var buf bytes.Buffer
	c.SetLogger(log.New(&buf, "", 0))
	return c, &buf
}

// assertNoBusy fails the test if the captured log contains any SQLite lock/busy
// error. The driver's error string is typically "database is locked (5)
// (SQLITE_BUSY)"; we check for both "locked" and "SQLITE_BUSY".
func assertNoBusy(t *testing.T, buf *bytes.Buffer) {
	t.Helper()
	s := buf.String()
	if strings.Contains(s, "locked") || strings.Contains(s, "SQLITE_BUSY") {
		t.Fatalf("SQLite lock/busy error surfaced under concurrency:\n%s", s)
	}
}

// TestConcurrentGlobalStatsWrites hammers the single-row stats table from many
// goroutines. Because RecordStats is an absolute-snapshot upsert, the final
// Stats() must equal exactly one writer's value (never a mixture), and no
// SQLITE_BUSY may surface.
func TestConcurrentGlobalStatsWrites(t *testing.T) {
	c, buf := captureLogger(t)
	const n = 50

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.RecordStats(Stats{EventsProcessedTotal: int64(i + 1)})
		}(i)
	}
	wg.Wait()

	s, ok := c.Stats()
	if !ok {
		t.Fatal("expected a stats row after concurrent writes")
	}
	// Absolute semantics: the value must be exactly one writer's value, not a
	// mixture or a partial write.
	if s.EventsProcessedTotal < 1 || s.EventsProcessedTotal > n {
		t.Fatalf("events = %d, want one of the written values 1..%d", s.EventsProcessedTotal, n)
	}
	assertNoBusy(t, buf)
}

// TestConcurrentFunctionStatsWrites writes distinct per-function rows from many
// goroutines concurrently, then verifies every row exists with its exact value.
func TestConcurrentFunctionStatsWrites(t *testing.T) {
	c, buf := captureLogger(t)
	const k = 30

	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "fn-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			c.RecordFunctionStats(FunctionStats{Function: name, EventsProcessedTotal: int64(i + 1)})
		}(i)
	}
	wg.Wait()

	all := c.AllFunctionStats()
	if len(all) != k {
		t.Fatalf("AllFunctionStats len = %d, want %d", len(all), k)
	}
	for _, fs := range all {
		want := int64(0)
		for i := 0; i < k; i++ {
			name := "fn-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			if fs.Function == name {
				want = int64(i + 1)
				break
			}
		}
		if fs.EventsProcessedTotal != want {
			t.Fatalf("function %q events = %d, want %d", fs.Function, fs.EventsProcessedTotal, want)
		}
	}
	assertNoBusy(t, buf)
}

// TestConcurrentReconcileAndStats runs reconcile-style writes (short tx) in one
// goroutine while another goroutine runs the stats-snapshot flush (short tx),
// then verifies the state is consistent and no lock/busy error surfaced.
func TestConcurrentReconcileAndStats(t *testing.T) {
	c, buf := captureLogger(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	const m = 8
	// Precompute the function values up front: fnFor calls t.TempDir(), which is
	// not safe to call from goroutines.
	fns := make([]function.Function, m)
	for i := 0; i < m; i++ {
		fns[i] = fnFor(t, "fn-"+string(rune('a'+i)), tmpl)
		c.RecordDiscovered(fns[i])
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// Reconcile-style writer: alternate success/skipped over the functions.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			for i := 0; i < m; i++ {
				name := "fn-" + string(rune('a'+i))
				if i%2 == 0 {
					c.RecordReconcileSuccess(name, "img", "fp", time.Now(), fns[i])
				} else {
					c.RecordSkipped(name)
				}
			}
		}
	}()

	// Stats-snapshot writer: short tx flush.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			fns := make([]FunctionStats, 0, m)
			for i := 0; i < m; i++ {
				fns = append(fns, FunctionStats{Function: "fn-" + string(rune('a'+i)), EventsProcessedTotal: int64(i + 1)})
			}
			_ = c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 100}, fns)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	// State must be consistent: every function present, global stats present.
	if got := len(c.ListFunctions()); got != m {
		t.Fatalf("ListFunctions len = %d, want %d", got, m)
	}
	if _, ok := c.Stats(); !ok {
		t.Fatal("expected global stats row after concurrent reconcile+flush")
	}
	if got := len(c.AllFunctionStats()); got != m {
		t.Fatalf("AllFunctionStats len = %d, want %d", got, m)
	}
	assertNoBusy(t, buf)
}

// TestConcurrentReadsDuringWrites runs a writer goroutine (stats snapshot) while
// reader goroutines call Stats/ListFunctions/GetFunction concurrently. Readers
// must return data or empty without panicking, and no lock/busy error may
// surface.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	c, buf := captureLogger(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordDiscovered(fnFor(t, "alpha", tmpl))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1 + 4)

	// Writer.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 1}, []FunctionStats{{Function: "alpha", EventsProcessedTotal: 1}})
		}
	}()

	// Readers.
	for r := 0; r < 4; r++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, _ = c.Stats()
				_ = c.ListFunctions()
				_, _ = c.GetFunction("alpha")
				_, _ = c.FunctionStats("alpha")
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()
	assertNoBusy(t, buf)
}

// TestReopenUnderConcurrency runs concurrent writes, closes, reopens the same
// path, and verifies the persisted values are correct.
func TestReopenUnderConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var buf bytes.Buffer
	c1.SetLogger(log.New(&buf, "", 0))

	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c1.RecordStats(Stats{EventsProcessedTotal: int64(i + 1)})
		}(i)
	}
	wg.Wait()
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	s, ok := c2.Stats()
	if !ok {
		t.Fatal("expected stats row after reopen")
	}
	if s.EventsProcessedTotal < 1 || s.EventsProcessedTotal > n {
		t.Fatalf("events = %d, want one of the written values 1..%d", s.EventsProcessedTotal, n)
	}
	assertNoBusy(t, &buf)
}

// TestPersistenceAfterConcurrency is a deterministic correctness check: distinct
// function_stats rows written concurrently plus one final global RecordStats
// must all be readable with exact values after Close+reopen.
func TestPersistenceAfterConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var buf bytes.Buffer
	c1.SetLogger(log.New(&buf, "", 0))

	const k = 20
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c1.RecordFunctionStats(FunctionStats{Function: "fn-" + string(rune('a'+i)), EventsProcessedTotal: int64(i + 1)})
		}(i)
	}
	wg.Wait()
	c1.RecordStats(Stats{EventsProcessedTotal: 999})
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	all := c2.AllFunctionStats()
	if len(all) != k {
		t.Fatalf("AllFunctionStats len = %d, want %d", len(all), k)
	}
	for _, fs := range all {
		want := int64(0)
		for i := 0; i < k; i++ {
			if fs.Function == "fn-"+string(rune('a'+i)) {
				want = int64(i + 1)
				break
			}
		}
		if fs.EventsProcessedTotal != want {
			t.Fatalf("function %q events = %d, want %d", fs.Function, fs.EventsProcessedTotal, want)
		}
	}
	gs, ok := c2.Stats()
	if !ok || gs.EventsProcessedTotal != 999 {
		t.Fatalf("global stats = %+v, ok=%v; want events 999", gs, ok)
	}
	assertNoBusy(t, &buf)
}

package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// statsJSONKeys is the exact set of payload keys expected in stats.data. It
// deliberately excludes the relational updated_at (and the id).
var statsJSONKeys = []string{
	"events_received_total",
	"events_matched_total",
	"events_unmatched_total",
	"handler_success_total",
	"handler_failure_total",
	"retry_total",
	"dlq_total",
	"pending_entries",
	"oldest_pending_age_seconds",
}

// functionStatsJSONRequiredKeys is the exact set of ALWAYS-present payload keys
// expected in function_stats.data: the counters (emitted even when 0, since
// they are absolute snapshots) plus the four execution-history timestamps. The
// timestamps are additionally omitempty: when a timestamp is empty the key is
// absent, which is what makes the flush merge preserve a stored value. The set
// deliberately excludes the relational function_name and updated_at, and the
// live pool gauges.
var functionStatsJSONRequiredKeys = []string{
	"events_matched_total",
	"handler_success_total",
	"handler_failure_total",
	"retry_total",
	"dlq_total",
	"warm_acquires_total",
	"cold_starts_total",
	"discarded_total",
}

// rawStatsData reads the stats.data column rendered back to JSON text (the same
// expression the production read path uses) plus updated_at for the single-row
// table. A stored value that is not valid JSON renders as an empty string.
func rawStatsData(t *testing.T, c *State) (data, updatedAt string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT CASE WHEN json_valid(data, 5) THEN json(data) END, updated_at FROM stats WHERE id = 1`).Scan(&data, &updatedAt); err != nil {
		t.Fatalf("read raw stats: %v", err)
	}
	return data, updatedAt
}

// rawFunctionStatsData reads function_stats.data rendered back to JSON text plus
// updated_at for name. A stored value that is not valid JSON renders as an empty
// string.
func rawFunctionStatsData(t *testing.T, c *State, name string) (data, updatedAt string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT CASE WHEN json_valid(data, 5) THEN json(data) END, updated_at FROM function_stats WHERE function_name = ?`, name).Scan(&data, &updatedAt); err != nil {
		t.Fatalf("read raw function stats: %v", err)
	}
	return data, updatedAt
}

// rawStatsBlob reads the stored stats.data blobs verbatim (no json() rendering)
// plus its SQLite storage type, so a test can assert the on-disk JSONB format
// and preserve a corrupt payload byte-for-byte.
func rawStatsBlob(t *testing.T, c *State) (data []byte, typeof, updatedAt string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT data, typeof(data), updated_at FROM stats WHERE id = 1`).Scan(&data, &typeof, &updatedAt); err != nil {
		t.Fatalf("read raw stats blob: %v", err)
	}
	return data, typeof, updatedAt
}

// rawFunctionStatsBlob is the per-function counterpart of rawStatsBlob.
func rawFunctionStatsBlob(t *testing.T, c *State, name string) (data []byte, typeof, updatedAt string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT data, typeof(data), updated_at FROM function_stats WHERE function_name = ?`, name).Scan(&data, &typeof, &updatedAt); err != nil {
		t.Fatalf("read raw function stats blob: %v", err)
	}
	return data, typeof, updatedAt
}

// assertJSONKeysExactly unmarshals data and fails unless its key set is exactly
// want (no more, no fewer).
func assertJSONKeysExactly(t *testing.T, data string, want []string) {
	t.Helper()
	assertJSONKeys(t, data, want, nil)
}

// assertJSONKeys unmarshals data and fails unless every required key is present
// and no key outside required ∪ allowed appears.
func assertJSONKeys(t *testing.T, data string, required, allowed []string) {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Fatalf("payload is not JSON: %q: %v", data, err)
	}
	known := make(map[string]bool, len(required)+len(allowed))
	for _, k := range required {
		known[k] = true
	}
	for _, k := range allowed {
		known[k] = true
	}
	for _, k := range required {
		if _, ok := m[k]; !ok {
			t.Errorf("payload missing key %q: %s", k, data)
		}
	}
	for k := range m {
		if !known[k] {
			t.Errorf("payload has unexpected key %q: %s", k, data)
		}
	}
}

// assertJSONKeysAbsent fails unless none of keys appear as top-level keys in the
// JSON payload. It is the key-set counterpart of assertJSONKeys for
// must-not-be-persisted fields (relations and live gauges), avoiding a bare
// substring match that a value could accidentally satisfy.
func assertJSONKeysAbsent(t *testing.T, data string, keys ...string) {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Fatalf("payload is not JSON: %q: %v", data, err)
	}
	for _, k := range keys {
		if _, ok := m[k]; ok {
			t.Errorf("payload must not carry key %q: %s", k, data)
		}
	}
}

// TestStatsPayloadIsCentralizedJSON pins the storage contract: the global stats
// row keeps only id/updated_at as columns, stores the whole counter/gauge
// payload as SQLite binary JSON (JSONB) with exactly the expected keys, and
// round-trips. The direct typeof(data) check pins the on-disk format.
func TestStatsPayloadIsCentralizedJSON(t *testing.T) {
	c := openTestState(t)
	in := Stats{
		EventsMatchedTotal:      12,
		HandlerSuccessTotal:     9,
		HandlerFailureTotal:     3,
		RetryTotal:              2,
		DLQTotal:                1,
		PendingEntries:          5,
		OldestPendingAgeSeconds: 42,
	}
	c.RecordStats(in)

	if _, typeof, _ := rawStatsBlob(t, c); typeof != "blob" {
		t.Fatalf("stats.data storage class = %q, want blob (JSONB)", typeof)
	}
	data, updatedAt := rawStatsData(t, c)
	assertJSONKeysExactly(t, data, statsJSONKeys)
	// updated_at is relational, not in the payload.
	assertJSONKeysAbsent(t, data, "updated_at")
	if updatedAt == "" {
		t.Fatal("stats.updated_at column must be set")
	}
	if _, err := time.Parse(time.RFC3339, updatedAt); err != nil {
		t.Fatalf("stats.updated_at not RFC3339: %q: %v", updatedAt, err)
	}

	got, ok := c.Stats()
	if !ok {
		t.Fatal("expected stats row")
	}
	in.UpdatedAt = updatedAt
	if got != in {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

// TestFunctionStatsPayloadIsCentralizedJSON pins the per-function storage
// contract: function_name and updated_at stay relational columns, the payload
// is SQLite binary JSON (JSONB) with exactly the expected keys (including the
// pool counters and timestamps), and the LIVE pool gauges are absent.
func TestFunctionStatsPayloadIsCentralizedJSON(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	in := FunctionStats{
		Function:            "alpha",
		EventsMatchedTotal:  5,
		HandlerSuccessTotal: 4,
		HandlerFailureTotal: 1,
		RetryTotal:          1,
		DLQTotal:            0,
		WarmAcquiresTotal:   7,
		ColdStartsTotal:     3,
		DiscardedTotal:      2,
		LastExecutionAt:     exec,
	}
	c.RecordFunctionStats(in)

	if _, typeof, _ := rawFunctionStatsBlob(t, c, "alpha"); typeof != "blob" {
		t.Fatalf("function_stats.data storage class = %q, want blob (JSONB)", typeof)
	}
	data, updatedAt := rawFunctionStatsData(t, c, "alpha")
	// last_execution_at is present (it was set); the other three timestamps are
	// empty and intentionally omitted.
	assertJSONKeys(t, data, functionStatsJSONRequiredKeys,
		[]string{"last_execution_at", "last_success_at", "last_failure_at", "last_dlq_at"})
	if !strings.Contains(data, `"last_execution_at"`) {
		t.Fatalf("expected the populated timestamp in the payload: %s", data)
	}
	for _, absent := range []string{`"last_success_at"`, `"last_failure_at"`, `"last_dlq_at"`} {
		if strings.Contains(data, absent) {
			t.Fatalf("empty timestamp %s must be omitted: %s", absent, data)
		}
	}
	// Relational metadata must not be duplicated in the payload. Use a key-set
	// check (not a bare substring) so a counter value that merely contains the
	// word cannot false-positive.
	assertJSONKeysAbsent(t, data, "function", "updated_at", "function_name")
	// The live pool gauges are never persisted.
	assertJSONKeysAbsent(t, data, "capacity", "containers", "busy", "idle", "starting")
	if updatedAt == "" {
		t.Fatal("function_stats.updated_at column must be set")
	}

	got, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected function stats row")
	}
	in.UpdatedAt = updatedAt
	if got != in {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
	// The function name comes from the relational key even though the payload
	// omits it.
	if got.Function != "alpha" {
		t.Fatalf("Function = %q, want alpha (from the key column)", got.Function)
	}
}

// TestStatsAbsentJSONFieldsZero pins the absent-field contract: a payload that
// omits fields decodes them as their Go zero value rather than erroring. This is
// a property of the current schema only; there is no migration or backward
// compatibility for a payload written by a different schema.
func TestStatsAbsentJSONFieldsZero(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO stats (id, data, updated_at) VALUES (1, jsonb(?), ?)`,
		`{"events_matched_total":7}`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed partial payload: %v", err)
	}

	s, ok := c.Stats()
	if !ok {
		t.Fatal("expected readable stats row")
	}
	if s.EventsMatchedTotal != 7 {
		t.Fatalf("events = %d, want 7", s.EventsMatchedTotal)
	}
	if s.HandlerSuccessTotal != 0 || s.HandlerFailureTotal != 0 || s.RetryTotal != 0 ||
		s.DLQTotal != 0 || s.PendingEntries != 0 || s.OldestPendingAgeSeconds != 0 {
		t.Fatalf("absent fields must decode to zero: %+v", s)
	}
	if s.UpdatedAt != "2020-01-01T00:00:00Z" {
		t.Fatalf("updated_at = %q, want the relational column value", s.UpdatedAt)
	}
}

// TestFunctionStatsAbsentJSONFieldsZero is the per-function counterpart: an
// empty payload object yields the zero FunctionStats (with Function filled from
// the key) and a partial payload zero-fills the absent timestamps.
func TestFunctionStatsAbsentJSONFieldsZero(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()
	for _, row := range []struct{ name, data string }{
		{"empty", `{}`},
		{"partial", `{"events_matched_total":7,"warm_acquires_total":2}`},
	} {
		if _, err := c.db.ExecContext(ctx,
			`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, jsonb(?), ?)`,
			row.name, row.data, "2020-01-01T00:00:00Z"); err != nil {
			t.Fatalf("seed %s: %v", row.name, err)
		}
	}

	e, ok := c.FunctionStats("empty")
	if !ok {
		t.Fatal("expected empty payload row to be readable")
	}
	if e != (FunctionStats{Function: "empty", UpdatedAt: "2020-01-01T00:00:00Z"}) {
		t.Fatalf("empty payload = %+v, want zero payload with metadata", e)
	}

	p, ok := c.FunctionStats("partial")
	if !ok {
		t.Fatal("expected partial payload row to be readable")
	}
	if p.EventsMatchedTotal != 7 || p.WarmAcquiresTotal != 2 {
		t.Fatalf("partial payload = %+v, want events 7 warm 2", p)
	}
	if p.LastExecutionAt != "" || p.LastSuccessAt != "" || p.LastFailureAt != "" || p.LastDLQAt != "" {
		t.Fatalf("absent timestamps must decode empty: %+v", p)
	}
}

// TestInvalidStatsJSONSurfacesErrors pins the invalid-payload contract: a
// corrupt stats.data is logged with a useful error and surfaced as unreadable
// (ok=false) rather than crashing or yielding a bogus snapshot.
func TestInvalidStatsJSONSurfacesErrors(t *testing.T) {
	c, buf := captureLogger(t)
	if _, err := c.db.ExecContext(context.Background(),
		`INSERT INTO stats (id, data, updated_at) VALUES (1, ?, ?)`,
		`{not-json`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt stats: %v", err)
	}

	if _, ok := c.Stats(); ok {
		t.Fatal("corrupt stats payload must read as unreadable (ok=false)")
	}
	logs := buf.String()
	if !strings.Contains(logs, "read stats failed") || !strings.Contains(logs, "unmarshal stats") {
		t.Fatalf("expected a useful decode error in the log, got:\n%s", logs)
	}
}

// TestInvalidFunctionStatsJSONSurfacesErrors pins the per-function counterpart:
// a corrupt payload is logged, FunctionStats reads as unreadable, and
// AllFunctionStats skips just the corrupt row while returning the good ones.
func TestInvalidFunctionStatsJSONSurfacesErrors(t *testing.T) {
	c, buf := captureLogger(t)
	ctx := context.Background()
	// The corrupt row is seeded as raw bytes bypassing jsonb() (which would
	// reject it at write time), simulating on-disk corruption; the good row is a
	// normal JSONB write.
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, ?, ?)`,
		"broken", `{not-json`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed broken: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, jsonb(?), ?)`,
		"good", `{"events_matched_total":3}`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed good: %v", err)
	}

	if _, ok := c.FunctionStats("broken"); ok {
		t.Fatal("corrupt function_stats payload must read as unreadable (ok=false)")
	}
	if _, ok := c.FunctionStats("good"); !ok {
		t.Fatal("good function_stats payload must remain readable")
	}

	all := c.AllFunctionStats()
	if len(all) != 1 || all[0].Function != "good" || all[0].EventsMatchedTotal != 3 {
		t.Fatalf("AllFunctionStats = %+v, want only the good row", all)
	}
	if logs := buf.String(); !strings.Contains(logs, "read function stats failed") || !strings.Contains(logs, "broken") {
		t.Fatalf("expected a useful per-function decode error in the log, got:\n%s", logs)
	}
}

// TestRecordFunctionStatsSelfHealsInvalidJSON verifies the read-modify-write
// upsert replaces a corrupt payload with the incoming absolute snapshot, so a
// bad row self-heals on the next record.
func TestRecordFunctionStatsSelfHealsInvalidJSON(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, ?, ?)`,
		"alpha", `{not-json`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt row: %v", err)
	}

	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsMatchedTotal: 4, LastExecutionAt: exec})

	got, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha to be readable after self-heal")
	}
	if got.EventsMatchedTotal != 4 || got.LastExecutionAt != exec {
		t.Fatalf("self-healed row = %+v, want events 4 execution %s", got, exec)
	}
}

// TestRecordStatsContextSelfHealsInvalidJSON is the global counterpart.
func TestRecordStatsContextSelfHealsInvalidJSON(t *testing.T) {
	c := openTestState(t)
	if _, err := c.db.ExecContext(context.Background(),
		`INSERT INTO stats (id, data, updated_at) VALUES (1, ?, ?)`,
		`{not-json`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt row: %v", err)
	}

	c.RecordStats(Stats{EventsMatchedTotal: 4})

	got, ok := c.Stats()
	if !ok || got.EventsMatchedTotal != 4 {
		t.Fatalf("self-healed stats = %+v, ok=%v; want events 4", got, ok)
	}
}

// TestStatsJSONReopenRoundTrip verifies the JSON payloads survive a Close/Open
// cycle with their counters, pool counters, and timestamps intact — the
// restart/restore contract for the new storage format.
func TestStatsJSONReopenRoundTrip(t *testing.T) {
	path := t.TempDir() + "/db.sqlite3"
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	global := Stats{EventsMatchedTotal: 100, PendingEntries: 9, OldestPendingAgeSeconds: 42}
	fn := FunctionStats{
		Function:           "alpha",
		EventsMatchedTotal: 10,
		WarmAcquiresTotal:  7,
		ColdStartsTotal:    3,
		DiscardedTotal:     2,
		LastExecutionAt:    exec,
		LastDLQAt:          exec,
	}
	c1.RecordStats(global)
	c1.RecordFunctionStats(fn)
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	gs, ok := c2.Stats()
	if !ok {
		t.Fatal("expected global stats after reopen")
	}
	global.UpdatedAt = gs.UpdatedAt
	if gs != global {
		t.Fatalf("global after reopen = %+v, want %+v", gs, global)
	}

	got, ok := c2.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats after reopen")
	}
	fn.UpdatedAt = got.UpdatedAt
	if got != fn {
		t.Fatalf("alpha after reopen = %+v, want %+v", got, fn)
	}
}

// TestRecordStatsSnapshotMergesTimestampsThroughJSON pins the flush merge over
// the JSON payload: an empty incoming timestamp preserves the stored one, while
// counters (including pool counters) are always replaced.
func TestRecordStatsSnapshotMergesTimestampsThroughJSON(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))
	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	c.RecordFunctionStats(FunctionStats{
		Function: "alpha", EventsMatchedTotal: 1, WarmAcquiresTotal: 7, LastExecutionAt: exec,
	})

	// A flush observing no timestamp must preserve the stored one, while the
	// absolute counters replace.
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsMatchedTotal: 2},
		[]FunctionStats{{Function: "alpha", EventsMatchedTotal: 2, WarmAcquiresTotal: 9}}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	got, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha after snapshot")
	}
	if got.LastExecutionAt != exec {
		t.Fatalf("LastExecutionAt = %q, want preserved %q", got.LastExecutionAt, exec)
	}
	if got.EventsMatchedTotal != 2 || got.WarmAcquiresTotal != 9 {
		t.Fatalf("counters = %+v, want events 2 warm 9 (absolute replace)", got)
	}
}

// TestRecordStatsSnapshotSelfHealsInvalidJSON verifies the flush's stored-payload
// read tolerates a corrupt row: it logs, treats it as zero, and the incoming
// absolute snapshot rewrites it.
func TestRecordStatsSnapshotSelfHealsInvalidJSON(t *testing.T) {
	c, buf := captureLogger(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))
	ctx := context.Background()
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, ?, ?)`,
		"alpha", `{not-json`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt row: %v", err)
	}

	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsMatchedTotal: 1},
		[]FunctionStats{{Function: "alpha", EventsMatchedTotal: 3, LastExecutionAt: exec}}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	got, ok := c.FunctionStats("alpha")
	if !ok || got.EventsMatchedTotal != 3 || got.LastExecutionAt != exec {
		t.Fatalf("self-healed row = %+v, ok=%v", got, ok)
	}
	if logs := buf.String(); !strings.Contains(logs, "read function stats failed") {
		t.Fatalf("expected a decode warning for the corrupt row, got:\n%s", logs)
	}
}

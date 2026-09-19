package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// statsJSONKeys is the exact set of payload keys expected in stats.data. It
// deliberately excludes the relational updated_at (and the id).
var statsJSONKeys = []string{
	"events_processed_total",
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
	"events_processed_total",
	"handler_success_total",
	"handler_failure_total",
	"retry_total",
	"dlq_total",
	"warm_acquires_total",
	"cold_starts_total",
	"discarded_total",
}

// rawStatsData reads the raw stats.data and updated_at columns for the
// single-row table.
func rawStatsData(t *testing.T, c *State) (data, updatedAt string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT data, updated_at FROM stats WHERE id = 1`).Scan(&data, &updatedAt); err != nil {
		t.Fatalf("read raw stats: %v", err)
	}
	return data, updatedAt
}

// rawFunctionStatsData reads the raw function_stats columns for name.
func rawFunctionStatsData(t *testing.T, c *State, name string) (data, updatedAt string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT data, updated_at FROM function_stats WHERE function_name = ?`, name).Scan(&data, &updatedAt); err != nil {
		t.Fatalf("read raw function stats: %v", err)
	}
	return data, updatedAt
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

// TestStatsPayloadIsCentralizedJSON pins the storage contract: the global stats
// row keeps only id/updated_at as columns, stores the whole counter/gauge
// payload as a JSON object with exactly the expected keys, and round-trips.
func TestStatsPayloadIsCentralizedJSON(t *testing.T) {
	c := openTestState(t)
	in := Stats{
		EventsProcessedTotal:    12,
		HandlerSuccessTotal:     9,
		HandlerFailureTotal:     3,
		RetryTotal:              2,
		DLQTotal:                1,
		PendingEntries:          5,
		OldestPendingAgeSeconds: 42,
	}
	c.RecordStats(in)

	data, updatedAt := rawStatsData(t, c)
	assertJSONKeysExactly(t, data, statsJSONKeys)
	// updated_at is relational, not in the payload.
	if strings.Contains(data, "updated_at") {
		t.Fatalf("payload must not carry updated_at: %s", data)
	}
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
// is JSON with exactly the expected keys (including the pool counters and
// timestamps), and the LIVE pool gauges are absent.
func TestFunctionStatsPayloadIsCentralizedJSON(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	in := FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 5,
		HandlerSuccessTotal:  4,
		HandlerFailureTotal:  1,
		RetryTotal:           1,
		DLQTotal:             0,
		WarmAcquiresTotal:    7,
		ColdStartsTotal:      3,
		DiscardedTotal:       2,
		LastExecutionAt:      exec,
	}
	c.RecordFunctionStats(in)

	data, updatedAt := rawFunctionStatsData(t, c, "alpha")
	// last_execution_at is present (it was set); the other three timestamps are
	// empty and intentionally omitted.
	assertJSONKeys(t, data, functionStatsJSONRequiredKeys, []string{"last_execution_at", "last_success_at", "last_failure_at", "last_dlq_at"})
	if !strings.Contains(data, `"last_execution_at"`) {
		t.Fatalf("expected the populated timestamp in the payload: %s", data)
	}
	for _, absent := range []string{`"last_success_at"`, `"last_failure_at"`, `"last_dlq_at"`} {
		if strings.Contains(data, absent) {
			t.Fatalf("empty timestamp %s must be omitted: %s", absent, data)
		}
	}
	// Relational metadata must not be duplicated in the payload.
	for _, forbidden := range []string{"function", "updated_at"} {
		if strings.Contains(data, `"`+forbidden+`"`) {
			t.Fatalf("payload must not carry relational %q: %s", forbidden, data)
		}
	}
	// The live pool gauges are never persisted.
	for _, gauge := range []string{"capacity", "containers", "busy", "idle", "starting"} {
		if strings.Contains(data, gauge) {
			t.Fatalf("payload must not carry live pool gauge %q: %s", gauge, data)
		}
	}
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

// TestStatsAbsentJSONFieldsZero pins the forward/backward-compatibility
// contract: a payload written without some fields (e.g. by an older or newer
// writer) decodes the absent fields as their Go zero value rather than erroring.
func TestStatsAbsentJSONFieldsZero(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO stats (id, data, updated_at) VALUES (1, ?, ?)`,
		`{"events_processed_total":7}`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed partial payload: %v", err)
	}

	s, ok := c.Stats()
	if !ok {
		t.Fatal("expected readable stats row")
	}
	if s.EventsProcessedTotal != 7 {
		t.Fatalf("events = %d, want 7", s.EventsProcessedTotal)
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
		{"partial", `{"events_processed_total":7,"warm_acquires_total":2}`},
	} {
		if _, err := c.db.ExecContext(ctx,
			`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, ?, ?)`,
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
	if p.EventsProcessedTotal != 7 || p.WarmAcquiresTotal != 2 {
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
	for _, row := range []struct{ name, data string }{
		{"broken", `{not-json`},
		{"good", `{"events_processed_total":3}`},
	} {
		if _, err := c.db.ExecContext(ctx,
			`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, ?, ?)`,
			row.name, row.data, "2020-01-01T00:00:00Z"); err != nil {
			t.Fatalf("seed %s: %v", row.name, err)
		}
	}

	if _, ok := c.FunctionStats("broken"); ok {
		t.Fatal("corrupt function_stats payload must read as unreadable (ok=false)")
	}
	if _, ok := c.FunctionStats("good"); !ok {
		t.Fatal("good function_stats payload must remain readable")
	}

	all := c.AllFunctionStats()
	if len(all) != 1 || all[0].Function != "good" || all[0].EventsProcessedTotal != 3 {
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
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 4, LastExecutionAt: exec})

	got, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha to be readable after self-heal")
	}
	if got.EventsProcessedTotal != 4 || got.LastExecutionAt != exec {
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

	c.RecordStats(Stats{EventsProcessedTotal: 4})

	got, ok := c.Stats()
	if !ok || got.EventsProcessedTotal != 4 {
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
	global := Stats{EventsProcessedTotal: 100, PendingEntries: 9, OldestPendingAgeSeconds: 42}
	fn := FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 10,
		WarmAcquiresTotal:    7,
		ColdStartsTotal:      3,
		DiscardedTotal:       2,
		LastExecutionAt:      exec,
		LastDLQAt:            exec,
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
		Function: "alpha", EventsProcessedTotal: 1, WarmAcquiresTotal: 7, LastExecutionAt: exec,
	})

	// A flush observing no timestamp must preserve the stored one, while the
	// absolute counters replace.
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 2},
		[]FunctionStats{{Function: "alpha", EventsProcessedTotal: 2, WarmAcquiresTotal: 9}}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	got, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha after snapshot")
	}
	if got.LastExecutionAt != exec {
		t.Fatalf("LastExecutionAt = %q, want preserved %q", got.LastExecutionAt, exec)
	}
	if got.EventsProcessedTotal != 2 || got.WarmAcquiresTotal != 9 {
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
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsProcessedTotal: 1},
		[]FunctionStats{{Function: "alpha", EventsProcessedTotal: 3, LastExecutionAt: exec}}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	got, ok := c.FunctionStats("alpha")
	if !ok || got.EventsProcessedTotal != 3 || got.LastExecutionAt != exec {
		t.Fatalf("self-healed row = %+v, ok=%v", got, ok)
	}
	if logs := buf.String(); !strings.Contains(logs, "read function stats failed") {
		t.Fatalf("expected a decode warning for the corrupt row, got:\n%s", logs)
	}
}

// TestStatsRelationalMetadataColumns pins the schema shape: only the stable
// metadata columns exist on stats/function_stats (plus data), so no counter is
// duplicated as a column.
func TestStatsRelationalMetadataColumns(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()

	statsCols, err := c.tableColumns(ctx, "stats")
	if err != nil {
		t.Fatalf("stats columns: %v", err)
	}
	for _, want := range []string{"id", "data", "updated_at"} {
		if !statsCols[want] {
			t.Errorf("stats missing metadata column %q: %v", want, statsCols)
		}
	}
	if len(statsCols) != 3 {
		t.Errorf("stats must have exactly id/data/updated_at, got %v", statsCols)
	}

	fnCols, err := c.tableColumns(ctx, "function_stats")
	if err != nil {
		t.Fatalf("function_stats columns: %v", err)
	}
	for _, want := range []string{"function_name", "data", "updated_at"} {
		if !fnCols[want] {
			t.Errorf("function_stats missing metadata column %q: %v", want, fnCols)
		}
	}
	if len(fnCols) != 3 {
		t.Errorf("function_stats must have exactly function_name/data/updated_at, got %v", fnCols)
	}
}

// TestOldSchemaStatsTablesAreNotMigrated documents the intentional removal of
// the stats migration helpers: CREATE TABLE IF NOT EXISTS leaves an old
// explicit-column stats table alone, so a legacy database does not silently gain
// the data column. This pins the "no stats backcompat" decision.
func TestOldSchemaStatsTablesAreNotMigrated(t *testing.T) {
	path := t.TempDir() + "/db.sqlite3"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("legacy open: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE stats (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			events_processed_total INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT
		)`); err != nil {
		t.Fatalf("legacy schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("legacy close: %v", err)
	}

	c, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c.Close()
	have, err := c.tableColumns(context.Background(), "stats")
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if have["data"] {
		t.Fatalf("old stats table must NOT be migrated to add data: %v", have)
	}
}

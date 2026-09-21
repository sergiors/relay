package state

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
)

func mustTemplate(t *testing.T, s string) *function.Template {
	t.Helper()
	tmpl, err := function.ParseTemplate([]byte(s))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

func fnFor(t *testing.T, name string, tmpl *function.Template) function.Function {
	t.Helper()
	return function.Function{Name: name, Dir: filepath.Join(t.TempDir(), name), Template: tmpl}
}

func openTestState(t *testing.T) *State {
	t.Helper()
	c, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	c.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return c
}

const twoHandlerTmpl = `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
    timeout: 6s
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
    timeout: 20s
`

// init is idempotent and re-opening the same file yields a working state DB.
func TestOpenIdempotentAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c1.RecordDiscovered(fnFor(t, "alpha", tmpl))
	_ = c1.Close()

	c2, err := Open(path) // reopen, init must be idempotent
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	if _, ok := c2.GetFunction("alpha"); !ok {
		t.Fatal("expected function to survive reopen")
	}
}

// discovered -> success: image/fingerprint/prepared/handlers are upserted.
func TestDiscoveredThenSuccessReplacesActiveFields(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	c.RecordDiscovered(fnFor(t, "fn", tmpl))

	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after discovered")
	}
	if d.Status != StatusPending {
		t.Fatalf("status = %s, want pending", d.Status)
	}
	if len(d.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(d.Handlers))
	}

	c.RecordReconcileSuccess("fn", "img-fn", "fp-new", time.Now(), fnFor(t, "fn", tmpl))

	d, ok = c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if d.Status != StatusReady {
		t.Fatalf("status = %s, want ready", d.Status)
	}
	if d.Image != "img-fn" {
		t.Fatalf("image = %q, want img-fn", d.Image)
	}
	if d.Fingerprint != "fp-new" {
		t.Fatalf("fingerprint = %q, want fp-new", d.Fingerprint)
	}
	if d.LastError != "" {
		t.Fatalf("last_error = %q, want cleared", d.LastError)
	}
	if len(d.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2 after success", len(d.Handlers))
	}
}

// KEY: a failed reconcile keeps the prior active image/fingerprint/prepared_at
// intact and only records the failure; status stays ready.
func TestReconcileFailureKeepsPriorActiveAndMarksFailed(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	prepared := time.Now().Add(-time.Hour)

	c.RecordReconcileSuccess("fn", "img-active", "fp-active", prepared, fnFor(t, "fn", tmpl))

	c.RecordReconcileFailure("fn", &boomErr{})

	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after failure")
	}
	if d.Status != StatusReady {
		t.Fatalf("status = %s, want ready (never marked unavailable)", d.Status)
	}
	if d.Image != "img-active" {
		t.Fatalf("image = %q, want preserved img-active", d.Image)
	}
	if d.Fingerprint != "fp-active" {
		t.Fatalf("fingerprint = %q, want preserved fp-active", d.Fingerprint)
	}
	if d.LastReconcileStatus != ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", d.LastReconcileStatus)
	}
	if d.LastError == "" {
		t.Fatal("last_error should be set on failure")
	}
	// The prepared_at of the active version is preserved, not overwritten.
	if !strings.HasPrefix(d.PreparedAt, prepared.UTC().Format(time.RFC3339)[:19]) {
		t.Fatalf("prepared_at = %s, want preserved active version time", d.PreparedAt)
	}
}

type boomErr struct{}

func (*boomErr) Error() string { return "boom" }

// TestRecordReconcileSuccessAdvancesLastReconcileAt is the regression for the
// upsert that omitted last_reconcile_at: a success on an EXISTING row (one
// already seeded by RecordDiscovered) must persist its reconcile timestamp and
// status. A second, later success must advance the timestamp. Time is driven by
// the injected State clock, so no sleep is needed: the first write is stamped at
// t1 and the second at t1+2s (RFC3339 has 1s resolution).
func TestRecordReconcileSuccessAdvancesLastReconcileAt(t *testing.T) {
	c := openTestState(t)
	clock := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c.nowFn = func() time.Time { return clock }
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	c.RecordDiscovered(fn) // existing row, empty reconcile fields

	img1, fp1 := "img-v1", "fp-v1"
	t1 := clock
	c.RecordReconcileSuccess("fn", img1, fp1, t1, fn)

	d1, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if d1.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", d1.LastReconcileStatus, ReconcileSuccess)
	}
	at1, err := time.Parse(time.RFC3339, d1.LastReconcileAt)
	if err != nil {
		t.Fatalf("first last_reconcile_at not a valid RFC3339 timestamp %q: %v", d1.LastReconcileAt, err)
	}

	// Advance the injected clock past the RFC3339 second resolution and record a
	// second success; the persisted timestamp must advance.
	clock = clock.Add(2 * time.Second)
	t2 := clock
	c.RecordReconcileSuccess("fn", img1, fp1, t2, fn)

	d2, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after second success")
	}
	if d2.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", d2.LastReconcileStatus, ReconcileSuccess)
	}
	at2, err := time.Parse(time.RFC3339, d2.LastReconcileAt)
	if err != nil {
		t.Fatalf("second last_reconcile_at not a valid RFC3339 timestamp %q: %v", d2.LastReconcileAt, err)
	}
	if !at2.After(at1) {
		t.Fatalf("last_reconcile_at did not advance: first=%s second=%s", at1, at2)
	}
}

// TestLastReconcileSurvivesDiscoveredUpsert pins the full upsert contract: (i) a
// success on an existing row persists its status and timestamp (the core fix),
// and (ii) a subsequent re-discovery upsert resets the outcome view (the
// excluded reconcile columns are empty), because a discovery is NOT a
// meaningful reconcile.
func TestLastReconcileSurvivesDiscoveredUpsert(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	c.RecordDiscovered(fn)

	// (i) success on the existing row persists status + timestamp.
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fn)
	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if d.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", d.LastReconcileStatus, ReconcileSuccess)
	}
	if d.LastReconcileAt == "" {
		t.Fatal("last_reconcile_at must be persisted by a success on an existing row")
	}

	// (ii) re-discovery resets the outcome view (it is not a meaningful reconcile).
	c.RecordDiscovered(fnFor(t, "fn", tmpl))
	d, ok = c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after re-discovery")
	}
	if d.LastReconcileStatus != "" {
		t.Fatalf("last_reconcile_status = %s after re-discovery, want empty", d.LastReconcileStatus)
	}
	if d.LastReconcileAt != "" {
		t.Fatalf("last_reconcile_at = %s after re-discovery, want empty", d.LastReconcileAt)
	}
}

// removal deletes the function row and its handlers.
func TestRemovalDeletesRowAndHandlers(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))

	c.RecordRemoved("fn")

	if _, ok := c.GetFunction("fn"); ok {
		t.Fatal("expected fn to be removed")
	}
	// Re-adding must not resurrect stale handlers (they were deleted).
	c.RecordDiscovered(fnFor(t, "fn", tmpl))
	if _, ok := c.GetFunction("fn"); !ok {
		t.Fatal("expected fn re-added")
	}
}

// ListFunctions returns rows sorted by name with the expected shape.
func TestListFunctionsShapeAndSort(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("zeta", "i-z", "f-z", time.Now(), fnFor(t, "zeta", tmpl))
	c.RecordDiscovered(fnFor(t, "alpha", tmpl))
	c.RecordReconcileSuccess("mid", "i-m", "f-m", time.Now(), fnFor(t, "mid", tmpl))

	rows := c.ListFunctions()
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0].Name != "alpha" || rows[1].Name != "mid" || rows[2].Name != "zeta" {
		t.Fatalf("rows not sorted by name: %+v", rows)
	}
	if rows[0].HandlerCount != 2 {
		t.Fatalf("handler count = %d, want 2", rows[0].HandlerCount)
	}
	if rows[1].Status != StatusReady {
		t.Fatalf("status = %s, want ready", rows[1].Status)
	}
}

// GetFunction of an unknown name returns false with an empty detail.
func TestGetFunctionUnknownReturnsFalse(t *testing.T) {
	c := openTestState(t)
	d, ok := c.GetFunction("nope")
	if ok {
		t.Fatalf("expected not found, got %+v", d)
	}
}

// RebuildFromFS populates an empty DB from a real functions tree using the real
// loader + fingerprint. A non-empty DB is left untouched.
func TestRebuildFromFSOnEmptyDB(t *testing.T) {
	root := t.TempDir()
	writeFunctionsDir(t, root)

	c := openTestState(t)
	if err := c.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	rows := c.ListFunctions()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Name != "demo" || rows[0].Runtime != "python3.14" || rows[0].Status != StatusPending {
		t.Fatalf("unexpected rebuild row: %+v", rows[0])
	}
	d, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo detail")
	}
	if len(d.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(d.Handlers))
	}

	// A second rebuild on a non-empty DB must not duplicate rows.
	if err := c.RebuildFromFS(root); err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if got := len(c.ListFunctions()); got != 1 {
		t.Fatalf("rebuild on non-empty DB changed row count to %d, want 1", got)
	}
}

// RecordReconcileSuccess round-trips a fingerprint-versioned image reference
// like "relay-fn-user-events:3f8a2c1d..." exactly, since the state DB is the
// authoritative persisted holder of the full fingerprinted image.
func TestRecordReconcileSuccessRoundTripsFingerprintedImage(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	img := "relay-fn-user-events:3f8a2c1d9b6e4a17"
	fp := "3f8a2c1d9b6e4a1700aa11bb22cc33dd44ee55ff66778899aabbccddeeff0011"
	c.RecordReconcileSuccess("user-events", img, fp, time.Now(), fnFor(t, "user-events", tmpl))

	d, ok := c.GetFunction("user-events")
	if !ok {
		t.Fatal("expected row after success")
	}
	if d.Image != img {
		t.Fatalf("image = %q, want %q", d.Image, img)
	}
	if d.Fingerprint != fp {
		t.Fatalf("fingerprint = %q, want %q", d.Fingerprint, fp)
	}
}

// relativeAgo renders short relative ages and falls back to absolute dates.
// Uses fixed Unix timestamps so sub-second truncation cannot make the test
// flaky: the "now" reference is chosen to land each age on an exact second.
func TestRelativeAgo(t *testing.T) {
	// Build a fixed "now" and fixed offsets; relativeAgo reads time.Now() so we
	// pin the inputs to exact rounded durations that won't drift across a second
	// boundary more than the (<=59s) tolerance.
	now := time.Now()
	mk := func(ago time.Duration) string {
		return time.Unix(0, now.Add(-ago).UnixNano()).UTC().Format(time.RFC3339)
	}
	cases := []struct {
		in   string
		want string
	}{
		{in: mk(15 * time.Second), want: "15s ago"},
		{in: mk(3 * time.Minute), want: "3m ago"},
		{in: mk(2 * time.Hour), want: "2h ago"},
		{in: mk(5 * 24 * time.Hour), want: "5d ago"},
		{in: mk(45 * 24 * time.Hour), want: time.Unix(0, now.Add(-45*24*time.Hour).UnixNano()).UTC().Format("2006-01-02")},
	}
	for _, tc := range cases {
		if got := RelativeAgo(tc.in); got != tc.want {
			t.Errorf("relativeAgo(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// PruneRemoved removes state for functions missing from the authoritative dir,
// INCLUDING handlers and function_stats, while keeping functions that still
// exist on disk and leaving the global stats row untouched.
func TestPruneRemovedSweepsStaleFunctions(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// "gone" exists only in the DB; "kept" exists on disk too. Give both state
	// rows (handlers + function_stats) so the sweep must clean them inclusively.
	c.RecordReconcileSuccess("gone", "img", "fp", time.Now(), fnFor(t, "gone", tmpl))
	c.RecordFunctionStats(FunctionStats{Function: "gone", EventsProcessedTotal: 5})
	c.RecordReconcileSuccess("kept", "img", "fp", time.Now(), fnFor(t, "kept", tmpl))
	c.RecordFunctionStats(FunctionStats{Function: "kept", EventsProcessedTotal: 9})
	// The global stats row must never be touched by pruning.
	want := Stats{EventsProcessedTotal: 55}
	c.RecordStats(want)

	// Real roots: create "kept", leave "gone" out, plus a stray non-function
	// file to confirm only dirs matter.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "kept"), 0o755); err != nil {
		t.Fatalf("mkdir kept: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write stray: %v", err)
	}

	c.PruneRemoved(root)

	// "gone" fully removed.
	if _, ok := c.GetFunction("gone"); ok {
		t.Fatal("gone function row must be pruned")
	}
	if _, ok := c.FunctionStats("gone"); ok {
		t.Fatal("gone function_stats must be pruned")
	}

	// "kept" survives with its function_stats.
	if _, ok := c.GetFunction("kept"); !ok {
		t.Fatal("kept function row must survive")
	}
	ks, ok := c.FunctionStats("kept")
	if !ok || ks.EventsProcessedTotal != 9 {
		t.Fatalf("kept function_stats = %+v, ok=%v; want events 9", ks, ok)
	}

	// Global stats untouched.
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("global stats row must survive pruning")
	}
	if gs.EventsProcessedTotal != 55 {
		t.Fatalf("global stats changed by prune: %+v", gs)
	}
}

// PruneRemoved is a no-op when the DB is empty and never drops a function that
// still exists on disk even if its state row predates the disk contents.
func TestPruneRemovedEmptyDBAndMissingDirName(t *testing.T) {
	c := openTestState(t)
	root := t.TempDir()
	c.PruneRemoved(root) // empty DB: no-op, must not error or log fatally

	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordDiscovered(fnFor(t, "demo", tmpl))
	if err := os.MkdirAll(filepath.Join(root, "demo"), 0o755); err != nil {
		t.Fatalf("mkdir demo: %v", err)
	}
	c.PruneRemoved(root)
	if _, ok := c.GetFunction("demo"); !ok {
		t.Fatal("demo must survive pruning while present on disk")
	}
}

// TestEnvSecretsMappingsPersisted verifies the env/secret MAPPINGS (never
// values) round-trip through the functions table, including env values that
// contain '=' (which is why JSON, not logfmt, is used).
func TestEnvSecretsMappingsPersisted(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, `runtime: python3.14
env:
  API_URL: https://api.example.com
  CONN: postgres://user:pass@host/db
secrets:
  DATABASE_URL: database-url
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))

	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if d.Env["API_URL"] != "https://api.example.com" {
		t.Errorf("env API_URL = %q, want https://api.example.com", d.Env["API_URL"])
	}
	// An env value containing '=' must survive intact.
	if d.Env["CONN"] != "postgres://user:pass@host/db" {
		t.Errorf("env CONN = %q, want the full connection string", d.Env["CONN"])
	}
	if d.Secrets["DATABASE_URL"] != "database-url" {
		t.Errorf("secrets DATABASE_URL = %q, want database-url (the reference, never a value)", d.Secrets["DATABASE_URL"])
	}
}

// TestEnvSecretsMappingsNilWhenAbsent verifies a template with no env/secrets
// yields nil maps (not empty non-nil maps).
func TestEnvSecretsMappingsNilWhenAbsent(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))

	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if d.Env != nil {
		t.Errorf("env = %v, want nil when absent", d.Env)
	}
	if d.Secrets != nil {
		t.Errorf("secrets = %v, want nil when absent", d.Secrets)
	}
}

// TestExecAddColumnToleratesDuplicateColumn pins the tolerance branch of the
// migration helper deterministically: execAddColumn is invoked for a column that
// ALREADY exists (the state a concurrent migrator leaves behind), so its ALTER
// fails with a duplicate-column error and the re-read must turn that into
// success. A still-missing column is returned as a genuine error.
//
// The exercised column (functions.env) belongs to the preserved
// functions-column migration; the removed stats migrations used to cover this
// branch, so the helper is pinned through an unrelated surviving column.
func TestExecAddColumnToleratesDuplicateColumn(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()

	// functions.env already exists (initSchema created it): a duplicate-column
	// ALTER must be tolerated.
	if err := c.execAddColumn(ctx, "functions", "env", "TEXT"); err != nil {
		t.Fatalf("execAddColumn on an existing column = %v; want nil (concurrent-win tolerance)", err)
	}

	// A genuine failure (a non-existent table) must still be returned: the
	// re-read cannot find the column, so the error is real, not a lost race.
	if err := c.execAddColumn(ctx, "no_such_table", "c", "TEXT"); err == nil {
		t.Fatal("execAddColumn on a missing table must return an error")
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
	path := filepath.Join(t.TempDir(), "db.sqlite3")
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

// TestEnvSecretsMigrationAddsColumns verifies a database created before the
// env/secrets columns existed is migrated idempotently: the columns are added
// and existing rows read back with nil maps.
func TestEnvSecretsMigrationAddsColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	// Create a DB with the OLD schema (no env/secrets columns).
	old, err := Open(path)
	if err != nil {
		t.Fatalf("open old: %v", err)
	}
	// Drop the columns to simulate a pre-migration database.
	if _, err := old.db.ExecContext(context.Background(), `ALTER TABLE functions DROP COLUMN env`); err != nil {
		t.Fatalf("drop env: %v", err)
	}
	if _, err := old.db.ExecContext(context.Background(), `ALTER TABLE functions DROP COLUMN secrets`); err != nil {
		t.Fatalf("drop secrets: %v", err)
	}
	_ = old.Close()

	// Reopen: the migration must re-add the columns.
	c, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c.Close()
	tmpl := mustTemplate(t, `runtime: python3.14
env:
  API_URL: https://api.example.com
secrets:
  DATABASE_URL: database-url
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))
	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after migration")
	}
	if d.Env["API_URL"] != "https://api.example.com" {
		t.Errorf("env API_URL = %q after migration, want https://api.example.com", d.Env["API_URL"])
	}
	if d.Secrets["DATABASE_URL"] != "database-url" {
		t.Errorf("secrets DATABASE_URL = %q after migration, want database-url", d.Secrets["DATABASE_URL"])
	}
}

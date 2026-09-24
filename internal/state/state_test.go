package state

import (
	"context"
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

	detail, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after discovered")
	}
	if detail.Status != StatusPending {
		t.Fatalf("status = %s, want pending", detail.Status)
	}
	if len(detail.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(detail.Handlers))
	}

	c.RecordReconcileSuccess("fn", "img-fn", "fp-new", time.Now(), fnFor(t, "fn", tmpl))

	detail, ok = c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.Status != StatusReady {
		t.Fatalf("status = %s, want ready", detail.Status)
	}
	if detail.Image != "img-fn" {
		t.Fatalf("image = %q, want img-fn", detail.Image)
	}
	if detail.Fingerprint != "fp-new" {
		t.Fatalf("fingerprint = %q, want fp-new", detail.Fingerprint)
	}
	if detail.LastError != "" {
		t.Fatalf("last_error = %q, want cleared", detail.LastError)
	}
	if len(detail.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2 after success", len(detail.Handlers))
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

	detail, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after failure")
	}
	if detail.Status != StatusReady {
		t.Fatalf("status = %s, want ready (never marked unavailable)", detail.Status)
	}
	if detail.Image != "img-active" {
		t.Fatalf("image = %q, want preserved img-active", detail.Image)
	}
	if detail.Fingerprint != "fp-active" {
		t.Fatalf("fingerprint = %q, want preserved fp-active", detail.Fingerprint)
	}
	if detail.LastReconcileStatus != ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", detail.LastReconcileStatus)
	}
	if detail.LastError == "" {
		t.Fatal("last_error should be set on failure")
	}
	// The prepared_at of the active version is preserved, not overwritten.
	if !strings.HasPrefix(detail.PreparedAt, prepared.UTC().Format(time.RFC3339)[:19]) {
		t.Fatalf("prepared_at = %s, want preserved active version time", detail.PreparedAt)
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
	detail, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", detail.LastReconcileStatus, ReconcileSuccess)
	}
	if detail.LastReconcileAt == "" {
		t.Fatal("last_reconcile_at must be persisted by a success on an existing row")
	}

	// (ii) re-discovery resets the outcome view (it is not a meaningful reconcile).
	c.RecordDiscovered(fnFor(t, "fn", tmpl))
	detail, ok = c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after re-discovery")
	}
	if detail.LastReconcileStatus != "" {
		t.Fatalf("last_reconcile_status = %s after re-discovery, want empty", detail.LastReconcileStatus)
	}
	if detail.LastReconcileAt != "" {
		t.Fatalf("last_reconcile_at = %s after re-discovery, want empty", detail.LastReconcileAt)
	}
}

// removal deletes the function row and its snapshot.
func TestRemovalDeletesRowAndHandlers(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))

	c.RecordRemoved("fn")

	if _, ok := c.GetFunction("fn"); ok {
		t.Fatal("expected fn to be removed")
	}
	// Re-adding must not resurrect stale handlers (the old snapshot was deleted).
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
	detail, ok := c.GetFunction("nope")
	if ok {
		t.Fatalf("expected not found, got %+v", detail)
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
	detail, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo detail")
	}
	if len(detail.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(detail.Handlers))
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

	detail, ok := c.GetFunction("user-events")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.Image != img {
		t.Fatalf("image = %q, want %q", detail.Image, img)
	}
	if detail.Fingerprint != fp {
		t.Fatalf("fingerprint = %q, want %q", detail.Fingerprint, fp)
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
// INCLUDING their snapshot and function_stats, while keeping functions that
// still exist on disk and leaving the global stats row untouched.
func TestPruneRemovedSweepsStaleFunctions(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// "gone" exists only in the DB; "kept" exists on disk too. Give both state
	// rows (snapshot + function_stats) so the sweep must clean them inclusively.
	c.RecordReconcileSuccess("gone", "img", "fp", time.Now(), fnFor(t, "gone", tmpl))
	c.RecordFunctionStats(FunctionStats{Function: "gone", EventsMatchedTotal: 5})
	c.RecordReconcileSuccess("kept", "img", "fp", time.Now(), fnFor(t, "kept", tmpl))
	c.RecordFunctionStats(FunctionStats{Function: "kept", EventsMatchedTotal: 9})
	// The global stats row must never be touched by pruning.
	want := Stats{EventsMatchedTotal: 55}
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
	if !ok || ks.EventsMatchedTotal != 9 {
		t.Fatalf("kept function_stats = %+v, ok=%v; want events 9", ks, ok)
	}

	// Global stats untouched.
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("global stats row must survive pruning")
	}
	if gs.EventsMatchedTotal != 55 {
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

	detail, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.Env["API_URL"] != "https://api.example.com" {
		t.Errorf("env API_URL = %q, want https://api.example.com", detail.Env["API_URL"])
	}
	// An env value containing '=' must survive intact.
	if detail.Env["CONN"] != "postgres://user:pass@host/db" {
		t.Errorf("env CONN = %q, want the full connection string", detail.Env["CONN"])
	}
	if detail.Secrets["DATABASE_URL"] != "database-url" {
		t.Errorf("secrets DATABASE_URL = %q, want database-url (the reference, never a value)", detail.Secrets["DATABASE_URL"])
	}
}

// TestEnvSecretsMappingsNilWhenAbsent verifies a template with no env/secrets
// yields nil maps (not empty non-nil maps).
func TestEnvSecretsMappingsNilWhenAbsent(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))

	detail, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.Env != nil {
		t.Errorf("env = %v, want nil when absent", detail.Env)
	}
	if detail.Secrets != nil {
		t.Errorf("secrets = %v, want nil when absent", detail.Secrets)
	}
}

// TestStatsRelationalMetadataColumns pins the schema shape: only the stable
// metadata columns exist on stats/function_stats (plus data), so no counter is
// duplicated as a column.
func TestStatsRelationalMetadataColumns(t *testing.T) {
	c := openTestState(t)

	statsCols := tableColumnSet(t, c, "stats")
	for _, want := range []string{"id", "data", "updated_at"} {
		if !statsCols[want] {
			t.Errorf("stats missing metadata column %q: %v", want, statsCols)
		}
	}
	if len(statsCols) != 3 {
		t.Errorf("stats must have exactly id/data/updated_at, got %v", statsCols)
	}

	fnCols := tableColumnSet(t, c, "function_stats")
	for _, want := range []string{"function_name", "data", "updated_at"} {
		if !fnCols[want] {
			t.Errorf("function_stats missing metadata column %q: %v", want, fnCols)
		}
	}
	if len(fnCols) != 3 {
		t.Errorf("function_stats must have exactly function_name/data/updated_at, got %v", fnCols)
	}
}

// tableColumnSet returns the set of column names of table using a zero-row
// SELECT, so tests can pin the current schema shape without a production
// schema-introspection helper.
func tableColumnSet(t *testing.T, c *State, table string) map[string]bool {
	t.Helper()
	rows, err := c.db.QueryContext(context.Background(), "SELECT * FROM "+table+" LIMIT 0")
	if err != nil {
		t.Fatalf("read %s columns: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns of %s: %v", table, err)
	}
	set := make(map[string]bool, len(cols))
	for _, col := range cols {
		set[col] = true
	}
	return set
}

// TestFreshSchemaHasCurrentColumns checks the current schema shape directly:
// initSchema must create the functions table with exactly name/data/updated_at,
// and must NOT create any of the removed child tables. The whole nested
// function configuration (handlers, schedules, services, env/secrets/networks)
// lives inside functions.data, so there are no per-handler/schedule/service
// tables.
func TestFreshSchemaHasCurrentColumns(t *testing.T) {
	c := openTestState(t)

	fnCols := tableColumnSet(t, c, "functions")
	for _, want := range []string{"name", "data", "updated_at"} {
		if !fnCols[want] {
			t.Errorf("functions missing current column %q: %v", want, fnCols)
		}
	}
	if len(fnCols) != 3 {
		t.Errorf("functions must have exactly name/data/updated_at, got %v", fnCols)
	}

	for _, table := range []string{"handlers", "schedules", "services"} {
		if tableExists(t, c, table) {
			t.Errorf("obsolete child table %q must not exist in the current schema", table)
		}
	}
}

// tableExists reports whether table is present in the database.
func tableExists(t *testing.T, c *State, table string) bool {
	t.Helper()
	var n int
	err := c.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&n)
	if err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	return n > 0
}

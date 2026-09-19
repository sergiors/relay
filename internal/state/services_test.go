package state

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const servicesTmpl = `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
services:
  - entrypoint: service.js
  - entrypoint: api.js
    port: 3000
    replicas: 3
`

// TestServiceRoundTrip seeds a template with services through
// RecordReconcileSuccess and asserts GetFunction returns the resolved rows
// (entrypoint file, effective port, desired replicas; defaults applied).
func TestServiceRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(d.Services))
	}
	// Rows are ordered by entrypoint, so api.js sorts before service.js.
	s0 := d.Services[0]
	if s0.Entrypoint != "api.js" || s0.Port != 3000 || s0.Replicas != 3 {
		t.Fatalf("service 0 = %+v, want api.js port=3000 replicas=3", s0)
	}
	s1 := d.Services[1]
	if s1.Entrypoint != "service.js" || s1.Port != 80 || s1.Replicas != 1 {
		t.Fatalf("service 1 = %+v, want defaults port=80 replicas=1", s1)
	}
}

// Template change replacing services updates the rows.
func TestServiceReplacementUpdatesRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	// Change: drop one service, change the other's port/replicas.
	changed := mustTemplate(t, `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
services:
  - entrypoint: api.js
    port: 8080
    replicas: 5
`)
	st.RecordReconcileSuccess("demo", "img2", "fp2", time.Now(), fnFor(t, "demo", changed))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 1 {
		t.Fatalf("services = %d, want 1 after replacement", len(d.Services))
	}
	s := d.Services[0]
	if s.Entrypoint != "api.js" || s.Port != 8080 || s.Replicas != 5 {
		t.Fatalf("service after change = %+v", s)
	}
}

// Removal clears the service rows.
func TestServiceRemovalClearsRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	st.RecordRemoved("demo")
	if _, ok := st.GetFunction("demo"); ok {
		t.Fatal("function row should be gone after removal")
	}
}

// A template without services stores none.
func TestServiceEmptyStored(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl) // no services key
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 0 {
		t.Fatalf("services = %d, want 0 for a template without services", len(d.Services))
	}
}

// TestServicesHandlerToEntrypointMigration verifies a database created before
// the services handler->entrypoint rename is migrated idempotently: the old
// services table (handler column) is rebuilt with an entrypoint column and an
// existing row's handler value survives, readable as Entrypoint.
func TestServicesHandlerToEntrypointMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")

	// Create a DB with the OLD services schema (handler column) and seed a row,
	// using database/sql + the modernc driver directly before opening State.
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open old: %v", err)
	}
	ctx := context.Background()
	// Replicate the old schema with just enough tables for initSchema to succeed
	// idempotently (CREATE TABLE IF NOT EXISTS skips existing tables).
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS functions (
			name TEXT PRIMARY KEY, runtime TEXT, status TEXT, image TEXT,
			fingerprint TEXT, prepared_at TEXT, last_reconcile_at TEXT,
			last_reconcile_status TEXT, last_error TEXT, updated_at TEXT,
			env TEXT, secrets TEXT)`,
		`CREATE TABLE IF NOT EXISTS handlers (function_name TEXT, handler TEXT, timeout TEXT, PRIMARY KEY (function_name, handler))`,
		`CREATE TABLE IF NOT EXISTS schedules (function_name TEXT, handler TEXT, cron TEXT, timezone TEXT, timeout TEXT, PRIMARY KEY (function_name, handler, cron, timezone))`,
		`CREATE TABLE IF NOT EXISTS services (function_name TEXT, handler TEXT, port INTEGER, replicas INTEGER, PRIMARY KEY (function_name, handler))`,
		`CREATE TABLE IF NOT EXISTS stats (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT, updated_at TEXT)`,
		`CREATE TABLE IF NOT EXISTS function_stats (function_name TEXT PRIMARY KEY, data TEXT, updated_at TEXT)`,
		`INSERT INTO services (function_name, handler, port, replicas) VALUES ('demo', 'service.js', 3000, 2)`,
	} {
		if _, err := old.ExecContext(ctx, stmt); err != nil {
			_ = old.Close()
			t.Fatalf("seed old schema: %v", err)
		}
	}
	_ = old.Close()

	// Open with the current State: initSchema must migrate handler->entrypoint.
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open migrated: %v", err)
	}
	defer st.Close()

	// The row survived and its handler value is now readable as Entrypoint.
	srows, err := st.db.QueryContext(ctx,
		`SELECT entrypoint, port, replicas FROM services WHERE function_name = 'demo'`)
	if err != nil {
		t.Fatalf("query migrated rows: %v", err)
	}
	defer srows.Close()
	var ep string
	var port, replicas int
	if !srows.Next() {
		t.Fatal("expected the seeded row to survive migration")
	}
	if err := srows.Scan(&ep, &port, &replicas); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if ep != "service.js" || port != 3000 || replicas != 2 {
		t.Fatalf("migrated row = %q %d %d, want service.js 3000 2", ep, port, replicas)
	}

	// Reopen again: the migration must be idempotent (a no-op) and the row must
	// still be there.
	_ = st.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	var ep2 string
	rerr := reopened.db.QueryRowContext(ctx,
		`SELECT entrypoint FROM services WHERE function_name = 'demo'`).Scan(&ep2)
	if rerr != nil || ep2 != "service.js" {
		t.Fatalf("row after idempotent reopen = %q, err=%v; want service.js", ep2, rerr)
	}
}

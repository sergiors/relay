package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// rawFunctionData reads functions.data rendered back to JSON text (the exact
// expression the production read path uses) plus the row's updated_at and the
// SQLite storage class of the raw blob.
func rawFunctionData(t *testing.T, c *State, name string) (data, updatedAt, typeof string) {
	t.Helper()
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT CASE WHEN json_valid(data, 5) THEN json(data) END, updated_at, typeof(data)
		 FROM functions WHERE name = ?`, name,
	).Scan(&data, &updatedAt, &typeof); err != nil {
		t.Fatalf("read raw function %q: %v", name, err)
	}
	return data, updatedAt, typeof
}

// fullSnapshotTmpl exercises every persisted snapshot field: runtime, env,
// secret references, networks, event handlers (name + timeout), schedules
// (handler/cron/timezone/timeout/retries), and services (all source kinds plus
// host/path/port/replicas).
const fullSnapshotTmpl = `runtime: python3.14
networks:
  - backend
  - frontend
env:
  API_URL: https://api.example.com
  CONN: postgres://user:pass@host/db
secrets:
  DATABASE_URL: database-url
events:
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
    timeout: 20s
    retries: 2
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
    timeout: 6s
schedules:
  - handler: jobs.report.handler
    cron: "0 8 * * 1-5"
    timezone: Europe/Rome
    timeout: 20s
    retries: 3
services:
  - entrypoint: service.js
    host: api.example.com
    path: /v2
    port: 3000
    replicas: 2
  - build: docker/Dockerfile.prod
    port: 8080
  - image: ghcr.io/acme/api:1.2
    port: 9090
`

// TestFunctionSnapshotIsJSONB pins the storage format directly: functions.data
// is a NOT NULL BLOB holding SQLite binary JSON (JSONB), so typeof(data) is
// "blob" and json_extract(data, ...) reads the relational-like fields straight
// out of the stored snapshot.
func TestFunctionSnapshotIsJSONB(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, fullSnapshotTmpl)
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	c.nowFn = func() time.Time { return base }
	c.RecordReconcileSuccess("demo", "img", "fp", base, fnFor(t, "demo", tmpl))

	data, updatedAt, typeof := rawFunctionData(t, c, "demo")
	if typeof != "blob" {
		t.Fatalf("functions.data storage class = %q, want blob (JSONB)", typeof)
	}
	if data == "" {
		t.Fatal("functions.data must render as JSON text")
	}
	if updatedAt != base.Format(time.RFC3339) {
		t.Fatalf("functions.updated_at = %q, want %q", updatedAt, base.Format(time.RFC3339))
	}

	// json_extract reads the payload's fields without decoding in Go, proving
	// the stored value is queryable JSONB.
	var runtime, status, image string
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT json_extract(data, '$.runtime'), json_extract(data, '$.status'), json_extract(data, '$.image')
		 FROM functions WHERE name = 'demo'`,
	).Scan(&runtime, &status, &image); err != nil {
		t.Fatalf("json_extract: %v", err)
	}
	if runtime != "python3.14" || status != StatusReady || image != "img" {
		t.Fatalf("json_extract = %q/%q/%q, want python3.14/ready/img", runtime, status, image)
	}

	// The nested configuration is queryable too: handler/schedule/service
	// counts AND a nested service's port, proving the payload is real JSONB.
	var handlers, schedules, services, entrypointPort int
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT json_array_length(json_extract(data, '$.handlers')),
		        json_array_length(json_extract(data, '$.schedules')),
		        json_array_length(json_extract(data, '$.services')),
		        json_extract(data, '$.services[1].port')
		 FROM functions WHERE name = 'demo'`,
	).Scan(&handlers, &schedules, &services, &entrypointPort); err != nil {
		t.Fatalf("json_extract nested: %v", err)
	}
	if handlers != 2 || schedules != 1 || services != 3 {
		t.Fatalf("nested counts = handlers %d schedules %d services %d; want 2/1/3", handlers, schedules, services)
	}
	if entrypointPort <= 0 {
		t.Fatalf("services[1].port = %d, want a positive configured port", entrypointPort)
	}

	// Relational metadata is not duplicated in the payload.
	assertFunctionJSONKeysAbsent(t, data, "name", "updated_at", "handler_count")
}

// TestFunctionSnapshotRoundTrip pins that the whole snapshot — the lifecycle
// fields and every nested configuration list — survives a full read/write round
// trip through JSONB.
func TestFunctionSnapshotRoundTrip(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, fullSnapshotTmpl)
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	c.nowFn = func() time.Time { return base }
	c.RecordReconcileSuccess("demo", "img", "fp", base, fnFor(t, "demo", tmpl))

	d, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo")
	}
	if d.Runtime != "python3.14" || d.Status != StatusReady || d.Image != "img" || d.Fingerprint != "fp" {
		t.Fatalf("lifecycle fields = %+v", d)
	}
	if d.PreparedAt != base.Format(time.RFC3339) || d.LastReconcileAt != base.Format(time.RFC3339) || d.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("reconcile fields = prepared %q at %q status %q", d.PreparedAt, d.LastReconcileAt, d.LastReconcileStatus)
	}

	// Handlers: name-ordered, exact timeouts, plus the retry budget.
	if len(d.Handlers) != 2 || d.Handlers[0].Name != "events.created.handler" || d.Handlers[0].Timeout != 6*time.Second {
		t.Fatalf("handlers = %+v", d.Handlers)
	}
	if d.Handlers[1].Name != "events.updated.handler" || d.Handlers[1].Timeout != 20*time.Second || d.Handlers[1].Retries != 2 {
		t.Fatalf("handler[1] = %+v", d.Handlers[1])
	}

	// Schedules: timezone, timeout, retries.
	if len(d.Schedules) != 1 {
		t.Fatalf("schedules = %+v", d.Schedules)
	}
	s := d.Schedules[0]
	if s.Handler != "jobs.report.handler" || s.Cron != "0 8 * * 1-5" || s.Timezone != "Europe/Rome" || s.Timeout != 20*time.Second || s.Retries != 3 {
		t.Fatalf("schedule = %+v", s)
	}

	// Services: all three source kinds plus host/path/port/replicas.
	if len(d.Services) != 3 {
		t.Fatalf("services = %+v", d.Services)
	}
	byEntry := map[string]Service{}
	for _, svc := range d.Services {
		byEntry[svc.Entrypoint+svc.Build+svc.Image] = svc
	}
	ep := byEntry["service.js"]
	if ep.Host != "api.example.com" || ep.Path != "/v2" || ep.Port != 3000 || ep.Replicas != 2 {
		t.Fatalf("entrypoint service = %+v", ep)
	}
	if b := byEntry["docker/Dockerfile.prod"]; b.Build != "docker/Dockerfile.prod" || b.Port != 8080 || b.Replicas != 1 {
		t.Fatalf("build service = %+v", b)
	}
	if im := byEntry["ghcr.io/acme/api:1.2"]; im.Image != "ghcr.io/acme/api:1.2" || im.Port != 9090 {
		t.Fatalf("image service = %+v", im)
	}

	// Env values (including one containing '=') survive; secrets hold only the
	// reference name, never a value.
	if d.Env["API_URL"] != "https://api.example.com" || d.Env["CONN"] != "postgres://user:pass@host/db" {
		t.Fatalf("env = %v", d.Env)
	}
	if d.Secrets["DATABASE_URL"] != "database-url" {
		t.Fatalf("secrets = %v", d.Secrets)
	}
	// Networks are normalized (sorted).
	if len(d.Networks) != 2 || d.Networks[0] != "backend" || d.Networks[1] != "frontend" {
		t.Fatalf("networks = %v", d.Networks)
	}

	// HandlerCount is derived from the snapshot on read.
	if d.HandlerCount != 2 {
		t.Fatalf("HandlerCount = %d, want 2", d.HandlerCount)
	}
}

// TestFunctionSnapshotAtomicReplacement verifies a template change replaces the
// whole snapshot in one row write: a dropped handler/schedule/service and
// changed fields are gone together, with no stale nested entries left behind.
func TestFunctionSnapshotAtomicReplacement(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, fullSnapshotTmpl)
	c.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	// Replace with a leaner template: one handler (new timeout), no schedules,
	// one service (new source/port), no networks, no secrets.
	changed := mustTemplate(t, `runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
    timeout: 30s
services:
  - entrypoint: api.js
    port: 7000
    replicas: 4
`)
	c.RecordReconcileSuccess("demo", "img2", "fp2", time.Now(), fnFor(t, "demo", changed))

	d, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo")
	}
	if d.Runtime != "node24" || d.Image != "img2" || d.Fingerprint != "fp2" {
		t.Fatalf("lifecycle not replaced: %+v", d)
	}
	if len(d.Handlers) != 1 || d.Handlers[0].Name != "index.main" || d.Handlers[0].Timeout != 30*time.Second {
		t.Fatalf("handlers not replaced: %+v", d.Handlers)
	}
	if len(d.Schedules) != 0 {
		t.Fatalf("stale schedules survived: %+v", d.Schedules)
	}
	if len(d.Services) != 1 || d.Services[0].Entrypoint != "api.js" || d.Services[0].Port != 7000 || d.Services[0].Replicas != 4 {
		t.Fatalf("services not replaced: %+v", d.Services)
	}
	if d.Env != nil || d.Secrets != nil || d.Networks != nil {
		t.Fatalf("stale env/secrets/networks survived: env=%v secrets=%v networks=%v", d.Env, d.Secrets, d.Networks)
	}

	// The replacement was a single-row UPDATE: the row count is unchanged.
	var n int
	if err := c.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM functions`).Scan(&n); err != nil {
		t.Fatalf("count functions: %v", err)
	}
	if n != 1 {
		t.Fatalf("functions rows = %d, want 1 (atomic replacement)", n)
	}
}

// TestFunctionRemovalDeletesSnapshotAndStats verifies removal deletes both the
// function snapshot row and its matching per-function stats row in one path.
func TestFunctionRemovalDeletesSnapshotAndStats(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, fullSnapshotTmpl)
	c.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))
	c.RecordFunctionStats(FunctionStats{Function: "demo", EventsMatchedTotal: 5})

	c.RecordRemoved("demo")

	if _, ok := c.GetFunction("demo"); ok {
		t.Fatal("functions row must be removed")
	}
	if _, ok := c.FunctionStats("demo"); ok {
		t.Fatal("function_stats row must be removed")
	}
	var n int
	if err := c.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM functions`).Scan(&n); err != nil {
		t.Fatalf("count functions: %v", err)
	}
	if n != 0 {
		t.Fatalf("functions rows = %d, want 0", n)
	}
}

// TestCorruptFunctionSnapshotFailsClearly pins the corrupt-payload contract for
// the functions snapshot: a stored data value that is not valid JSON cannot be
// decoded, so GetFunction reports unknown (ok=false) and ListFunctions skips the
// bad row, both logging a clear decode error rather than panicking or returning
// a partial record. The corrupt write bypasses jsonb() (which validates on
// write) to simulate on-disk corruption.
func TestCorruptFunctionSnapshotFailsClearly(t *testing.T) {
	c, buf := captureLogger(t)
	ctx := context.Background()
	// A valid JSONB row and a corrupt raw row.
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO functions (name, data, updated_at) VALUES ('good', jsonb(?), ?)`,
		`{"runtime":"node24","handlers":[{"name":"a.b","timeout":1000000000}]}`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed good: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO functions (name, data, updated_at) VALUES ('broken', ?, ?)`,
		`{not-json`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed broken: %v", err)
	}

	if _, ok := c.GetFunction("broken"); ok {
		t.Fatal("corrupt function snapshot must read as unknown (ok=false)")
	}
	if _, ok := c.GetFunction("good"); !ok {
		t.Fatal("good function snapshot must remain readable")
	}

	rows := c.ListFunctions()
	if len(rows) != 1 || rows[0].Name != "good" || rows[0].Runtime != "node24" {
		t.Fatalf("ListFunctions = %+v, want only the good row", rows)
	}

	logs := buf.String()
	if !strings.Contains(logs, "read function payload failed") && !strings.Contains(logs, "get failed") {
		t.Fatalf("expected a useful decode error in the log, got:\n%s", logs)
	}
	if !strings.Contains(logs, "broken") {
		t.Fatalf("expected the corrupt function name in the log, got:\n%s", logs)
	}
}

// TestReconcileFailurePreservesSnapshotOnAbsentRow pins that a failure for an
// unknown function is a no-op (no row created), matching the former
// UPDATE ... WHERE name = ? semantics.
func TestReconcileFailurePreservesSnapshotOnAbsentRow(t *testing.T) {
	c := openTestState(t)
	c.RecordReconcileFailure("ghost", &boomErr{})
	if _, ok := c.GetFunction("ghost"); ok {
		t.Fatal("failure for an unknown function must not create a row")
	}
}

// TestReconcileFailurePreservesNestedSnapshot verifies a failed rebuild keeps
// the whole prior snapshot — including the nested handler/schedule/service
// lists — and only records the failure outcome.
func TestReconcileFailurePreservesNestedSnapshot(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, fullSnapshotTmpl)
	c.RecordReconcileSuccess("demo", "img-active", "fp-active", time.Now(), fnFor(t, "demo", tmpl))

	before, _ := c.GetFunction("demo")
	c.RecordReconcileFailure("demo", &boomErr{})
	after, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("row must survive a failed reconcile")
	}
	if after.Status != StatusReady || after.Image != "img-active" || after.Fingerprint != "fp-active" {
		t.Fatalf("active fields changed: %+v", after)
	}
	if after.LastReconcileStatus != ReconcileFailed || after.LastError != "boom" {
		t.Fatalf("failure outcome not recorded: status=%q error=%q", after.LastReconcileStatus, after.LastError)
	}
	if len(after.Handlers) != len(before.Handlers) || len(after.Schedules) != len(before.Schedules) || len(after.Services) != len(before.Services) {
		t.Fatalf("nested snapshot changed: before=%+v after=%+v", before, after)
	}
	if len(after.Networks) != len(before.Networks) || after.Secrets["DATABASE_URL"] != before.Secrets["DATABASE_URL"] {
		t.Fatalf("networks/secrets changed: before=%v/%v after=%v/%v", before.Networks, before.Secrets, after.Networks, after.Secrets)
	}
}

// assertFunctionJSONKeysAbsent fails unless none of keys appear as top-level
// keys in the JSON payload.
func assertFunctionJSONKeysAbsent(t *testing.T, data string, keys ...string) {
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

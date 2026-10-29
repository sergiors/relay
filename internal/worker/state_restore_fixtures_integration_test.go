//go:build integration

package worker

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/state"
	"relay/internal/testutil"
)

// newRawStateConn opens a raw PostgreSQL connection on the same schema as dsn,
// so a test can plant a payload directly (bypassing the state package's
// marshalling) to exercise the corrupt-payload read path. A JSONB column can
// never hold invalid JSON, so "corrupt" here means a well-formed JSON value of
// the wrong shape.
func newRawStateConn(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedCorruptGlobalRow writes a wrong-shape global stats payload directly into
// the state DB, for this handle's node row.
func seedCorruptGlobalRow(t *testing.T, dsn, worker string) {
	t.Helper()
	db := newRawStateConn(t, dsn)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stats (node_id, data, updated_at) VALUES ($1, $2, $3)`,
		worker, `"not-an-object"`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt stats row: %v", err)
	}
}

// seedCorruptAppRow writes a wrong-shape app_stats payload directly into the
// state DB for app name, owned by worker.
func seedCorruptAppRow(t *testing.T, dsn, worker, name string) {
	t.Helper()
	db := newRawStateConn(t, dsn)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO app_stats (app_name, node_id, data, updated_at) VALUES ($1, $2, $3, $4)`,
		name, worker, `"not-an-object"`, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt app_stats row: %v", err)
	}
}

// openTempState opens a state DB on a fresh isolated schema and registers its
// close. It fails (never skips) when PostgreSQL is unreachable — testutil.NewState
// creates the per-test schema as the availability check — while unit tests must
// not depend on the database, so only integration-tagged tests call it.
func openTempState(t *testing.T) *state.State {
	t.Helper()
	return testutil.NewState(t)
}

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

// openTempWorkerNodeState opens a state handle scoped to a fresh unique node
// identity — the shape every production worker handle has (NodeID + Instance) —
// and returns it with its DSN. Node-scoped resets (ResetNodeStats through the
// flusher's ResetStats) and the instance fence only exist for such handles, so
// flush/reset tests exercise them here instead of on the identity-free
// openTempState. The identity is unique per test because PostgreSQL session
// advisory locks are database-wide: per-test schemas do not namespace them.
func openTempWorkerNodeState(t *testing.T) (*state.State, string) {
	t.Helper()
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn, state.Options{
		NodeID:   testutil.UniqueName(t, "stats-node"),
		Instance: "inst-1",
	})
	return st, dsn
}

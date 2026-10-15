package testutil

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/state"
)

// TestDatabaseURL returns the PostgreSQL DSN the suites use for the state
// database, from TEST_DATABASE_URL. The default matches the CI service container
// (localhost:5432, user/password/database relay) so the suite runs with no extra
// configuration where such a server exists; set TEST_DATABASE_URL to point at
// any reachable PostgreSQL.
func TestDatabaseURL() string {
	return EnvOr("TEST_DATABASE_URL", "postgres://relay:relay@localhost:5432/relay?sslmode=disable")
}

// RequireStateDatabase fails the test when PostgreSQL is unreachable. It is used
// by the integration-tagged tests, which are meaningless without a real
// database (they prove row locking, claim fencing, and cross-process
// coordination): a missing dependency fails rather than skips, matching the
// Redis/Docker integration convention.
func RequireStateDatabase(t *testing.T) {
	t.Helper()
	db, err := sql.Open("pgx", TestDatabaseURL())
	if err != nil {
		t.Fatalf("state integration test requires PostgreSQL (open: %v)", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf(
			"state integration test requires a reachable PostgreSQL at TEST_DATABASE_URL (ping: %v); "+
				"start one and point TEST_DATABASE_URL at it",
			err,
		)
	}
}

// stateDatabaseAvailable reports whether PostgreSQL is reachable, so the
// converted (untagged) state tests can skip instead of failing when a developer
// runs the default unit suite with no database. CI provisions PostgreSQL for
// the integration job, where these tests run for real.
func stateDatabaseAvailable(t *testing.T) bool {
	t.Helper()
	db, err := sql.Open("pgx", TestDatabaseURL())
	if err != nil {
		return false
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx) == nil
}

// NewStateDSN creates a fresh, uniquely-named schema inside the test database
// and returns a DSN pinned to it via search_path, plus a cleanup that drops the
// schema. Every test gets its own namespace, so tests never share or
// destructively mutate another test's tables — the tables are created inside the
// per-test schema by state.Open. The schema is dropped (CASCADE) on cleanup,
// after every state handle opened on it has closed (handles registered later in
// the same test close first, cleanup being LIFO).
//
// A caller that only needs an isolated namespace (not a handle) can use the
// returned DSN with state.Open directly, or the OpenState/NewState helpers.
func NewStateDSN(t *testing.T) string {
	t.Helper()
	base := TestDatabaseURL()
	if !stateDatabaseAvailable(t) {
		t.Skipf("state tests require PostgreSQL at TEST_DATABASE_URL (%s); none reachable", base)
	}
	schema := uniqueSchemaName()

	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+quoteIdent(schema)); err != nil {
		_ = admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropCtx, `DROP SCHEMA IF EXISTS `+quoteIdent(schema)+` CASCADE`); err != nil {
			t.Logf("drop test schema %s: %v", schema, err)
		}
		_ = admin.Close()
	})
	return withSearchPath(base, schema)
}

// OpenState opens a state handle on the supplied DSN (typically from
// NewStateDSN) and registers its close. Optional options are applied over an
// empty Options, so a caller can set the stats Owner identity.
func OpenState(t *testing.T, dsn string, opts ...state.Options) *state.State {
	t.Helper()
	var o state.Options
	if len(opts) > 0 {
		o = opts[0]
	}
	o.DSN = dsn
	st, err := state.Open(context.Background(), o)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// NewState opens a state handle on a fresh isolated schema.
func NewState(t *testing.T, opts ...state.Options) *state.State {
	t.Helper()
	return OpenState(t, NewStateDSN(t), opts...)
}

// uniqueSchemaName returns a process-unique schema name, so concurrent tests
// (and parallel packages) never collide.
func uniqueSchemaName() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to a time-based name; collisions are astronomically
		// unlikely and a failure is not worth failing the test over.
		return fmt.Sprintf("relay_t_%d", time.Now().UnixNano())
	}
	return "relay_t_" + hex.EncodeToString(b[:])
}

// quoteIdent double-quotes a schema identifier. The names are generated here
// (hex + a fixed prefix), so this is defense in depth rather than a general
// identifier-quoting utility.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// withSearchPath returns base with search_path set to schema, preserving any
// existing query parameters. pgx sends search_path in the startup packet, so
// every pooled connection resolves unqualified table names inside the schema.
// Both URL ("postgres://...?k=v") and keyword/value ("host=... user=...") DSN
// forms are supported.
func withSearchPath(base, schema string) string {
	if strings.HasPrefix(base, "postgres://") || strings.HasPrefix(base, "postgresql://") {
		u, err := url.Parse(base)
		if err == nil {
			q := u.Query()
			q.Set("search_path", schema)
			u.RawQuery = q.Encode()
			return u.String()
		}
		return base + "?search_path=" + url.QueryEscape(schema)
	}
	return base + " search_path=" + schema
}

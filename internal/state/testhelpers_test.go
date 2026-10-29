//go:build integration

package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/app"
)

// The state tests run against a real PostgreSQL instance (the suite is a
// database integration suite). TEST_DATABASE_URL selects it; the default
// matches the CI service container. The real-database tests are excluded from
// the default suite by the integration build tag, and when that suite runs they
// FAIL rather than skip if no database is reachable: they are meaningless
// without PostgreSQL. TEST_DATABASE_URL never appears in diagnostics; valid
// URL DSNs report only host:port, and keyword/value or malformed DSNs report a
// generic non-secret label.
//
// Every test gets its own uniquely-named schema (search_path-pinned), so tests
// never share tables or destructively mutate one another. The schema is dropped
// on cleanup; because cleanup runs LIFO, handles opened after newTestDSN close
// before the schema is dropped.

func testDatabaseURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://relay:relay@localhost:5432/relay?sslmode=disable"
}

func testDatabaseAvailable(t *testing.T) bool {
	t.Helper()
	db, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		return false
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx) == nil
}

// testDatabaseEndpoint describes the configured server without credentials, so
// a missing-database failure stays actionable without leaking a password.
func testDatabaseEndpoint() string {
	return safeDatabaseEndpoint(testDatabaseURL())
}

// newTestDSN creates a fresh isolated schema and returns a DSN pinned to it.
// It fails (never skips) when PostgreSQL is unreachable: only
// integration-tagged tests call it, and they require a real database.
func newTestDSN(t *testing.T) string {
	t.Helper()
	base := testDatabaseURL()
	if !testDatabaseAvailable(t) {
		t.Fatalf("state integration test requires a reachable PostgreSQL at %s "+
			"(from TEST_DATABASE_URL); start one and point TEST_DATABASE_URL at it",
			testDatabaseEndpoint())
	}
	schema := "relay_test_" + randHex(t)
	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+quoteSchemaIdent(schema)); err != nil {
		_ = admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropCtx, `DROP SCHEMA IF EXISTS `+quoteSchemaIdent(schema)+` CASCADE`); err != nil {
			t.Logf("drop test schema %s: %v", schema, err)
		}
		_ = admin.Close()
	})
	return testWithSearchPath(base, schema)
}

// testOpen opens a state handle on dsn and registers its close. It mirrors the
// production Open signature shape (*State, error) so call sites that already
// handle the error read unchanged.
func testOpen(t *testing.T, dsn string) (*State, error) {
	t.Helper()
	st, err := Open(context.Background(), Options{DSN: dsn})
	if err == nil {
		t.Cleanup(func() { _ = st.Close() })
	}
	return st, err
}

func randHex(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func quoteSchemaIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func testWithSearchPath(base, schema string) string {
	if strings.HasPrefix(base, "postgres://") || strings.HasPrefix(base, "postgresql://") {
		if u, err := url.Parse(base); err == nil {
			q := u.Query()
			q.Set("search_path", schema)
			u.RawQuery = q.Encode()
			return u.String()
		}
		return base + "?search_path=" + url.QueryEscape(schema)
	}
	return base + " search_path=" + schema
}

// openTestState is the shared helper most tests use: a fresh isolated schema
// with a debug logger installed.
func openTestState(t *testing.T) *State {
	t.Helper()
	c := testOpenOrFail(t, newTestDSN(t))
	c.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return c
}

func testOpenOrFail(t *testing.T, dsn string) *State {
	t.Helper()
	c, err := testOpen(t, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return c
}

// openUnlockedState opens a State on dsn scoped to a node identity WITHOUT
// taking the per-node startup lock (see Open/acquireNodeLock). Production
// handles always take the lock, so two live handles for one node can no longer
// coexist; this helper lets tests simulate a second concurrent incarnation of
// the same node at the DATABASE layer to exercise the residual stats
// incarnation fence independently of the process-level exclusivity. It still
// performs the stats-ownership claim Open performs, so a "new incarnation" can
// take over the row exactly as a real restart would. The schema is initialized
// idempotently, so the helper is self-contained.
func openUnlockedState(t *testing.T, dsn, nodeID, instance string) *State {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open unlocked state: %v", err)
	}
	st := &State{db: db, log: fallbackLogger, nodeID: nodeID, instance: instance}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	if err := st.initSchema(ctx); err != nil {
		t.Fatalf("open unlocked state: init schema: %v", err)
	}
	if err := st.claimStatsOwnership(ctx, st.db); err != nil {
		t.Fatalf("open unlocked state: claim: %v", err)
	}
	return st
}

// seedFencingToken persists fn's desired generation and returns the exact
// fencing token that write stored. Guarded writes (success, failure, status,
// removal) must carry this fencing token: zero is a failed desired write and
// never persists.
func seedFencingToken(t *testing.T, c *State, fn app.App) FencingToken {
	t.Helper()
	fencingToken, err := c.RecordDiscovered(context.Background(), fn)
	return requireFencingTokenResult(t, fn.Name, fencingToken, err)
}

// seedFencingTokenWithFingerprint is seedFencingToken with a caller-supplied desired
// fingerprint.
func seedFencingTokenWithFingerprint(t *testing.T, c *State, fn app.App, fingerprint string) FencingToken {
	t.Helper()
	fencingToken, err := c.RecordDiscoveredWithFingerprint(context.Background(), fn, fingerprint)
	return requireFencingTokenResult(t, fn.Name, fencingToken, err)
}

func requireDesired(t *testing.T, name string, write func() (FencingToken, error)) FencingToken {
	t.Helper()
	fencingToken, err := write()
	return requireFencingTokenResult(t, name, fencingToken, err)
}

func requireFencingTokenResult(t *testing.T, name string, fencingToken FencingToken, err error) FencingToken {
	t.Helper()
	if err != nil {
		t.Fatalf("desired-generation write for %q: %v", name, err)
	}
	return requireFencingToken(t, name, fencingToken)
}

func requireWrite(t *testing.T, name string, write func() (bool, error)) bool {
	t.Helper()
	landed, err := write()
	if err != nil {
		t.Fatalf("state write for %q: %v", name, err)
	}
	return landed
}

func requireFencingToken(t *testing.T, name string, fencingToken FencingToken) FencingToken {
	t.Helper()
	if fencingToken <= 0 {
		t.Fatalf("desired-generation write for %q returned fencing token %d, want positive",
			name, fencingToken)
	}
	return fencingToken
}

// absentFencingToken is a positive fencing token for writes that must not
// affect a row. It is far beyond any value the test schema's sequence
// allocates, so it bypasses the zero-token short-circuit and exercises the
// fencing guard's stale/absent-row path without colliding with a real token.
func absentFencingToken() FencingToken { return FencingToken(1 << 62) }

// testSyncBuffer is a mutex-protected log buffer for tests whose logger is
// written by background goroutines while the test goroutine asserts on it. It
// mirrors testutil.SyncBuffer; the state package's own tests cannot import
// testutil (testutil imports state, which would be an import cycle).
type testSyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *testSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *testSyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

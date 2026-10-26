package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

// AppNames is the live-set reader behind the worker flush's metric sweep (see
// app_stats.go and metrics.SweepAppMetrics). Its ok flag is load-bearing: the
// worker's sweep must treat ok=false as an UNKNOWN live set and fail open —
// retaining every series — because sweeping against an empty (or partial)
// name list would delete live apps' series. These tests pin the complete /
// empty / cancelled / mid-iteration-failure contracts of that flag.
//
// The real-PostgreSQL complete/empty/cancelled contracts live in the
// integration-tagged app_names_integration_test.go; the mid-iteration test
// below uses a narrowly scoped test-only database/sql driver because a real
// server cannot deterministically fail Rows.Next after a fixed number of
// successfully scanned rows.

// --- Test-only driver stub for the mid-iteration failure test ---

// errRowsIterationInjected is the sentinel the stub surfaces on Rows.Next
// AFTER its successfully readable rows are exhausted; database/sql turns a
// non-EOF error into what rows.Err() reports at the end of iteration — exactly
// the shape of a connection dying or the server erroring mid-stream.
var errRowsIterationInjected = errors.New("injected iteration failure (rows.Err)")

// stubNameRows is a driver.Rows returning rowsToRead then the injected error.
type stubNameRows struct {
	rows [][]driver.Value
	next int
}

func (r *stubNameRows) Columns() []string { return []string{"name"} }
func (r *stubNameRows) Close() error      { return nil }
func (r *stubNameRows) Next(dest []driver.Value) error {
	if r.next < len(r.rows) {
		copy(dest, r.rows[r.next])
		r.next++
		return nil
	}
	return errRowsIterationInjected
}

// stubNamesConn answers the AppNames query with the stub rows and refuses
// everything else the database/sql machinery might try (prepare/tx), so any
// unexpected code path fails loudly instead of passing silently.
type stubNamesConn struct{ rows driver.Rows }

func (c *stubNamesConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("stubNamesConn: prepare unsupported")
}
func (c *stubNamesConn) Close() error { return nil }
func (c *stubNamesConn) Begin() (driver.Tx, error) {
	return nil, errors.New("stubNamesConn: tx unsupported")
}
func (c *stubNamesConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}

// stubNamesConnector hands every connection the same rows. AppNames queries
// once per call, so sharing is safe here; the rows object carries the
// consumed-position state.
type stubNamesConnector struct{ rows driver.Rows }

func (c stubNamesConnector) Connect(context.Context) (driver.Conn, error) {
	return &stubNamesConn{rows: c.rows}, nil
}
func (c stubNamesConnector) Driver() driver.Driver { return stubNamesDriver{} }

type stubNamesDriver struct{}

func (stubNamesDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("stubNamesDriver: use OpenDB with the connector")
}

// TestAppNamesMidIterationErrorReportsFailure is the FT-001 regression: a read
// that fails WHILE iterating (after one or more rows were already scanned) is
// a FAILED read — ok=false with no names — never a shorter-but-complete live
// set. Sweeping against the partial set {alpha} would delete every series the
// read had not yet reached (ghost), so the partial data must be discarded and
// the caller must be able to distinguish this from a complete result.
//
// A real server disconnect is not deterministic enough to guarantee WHERE the
// iteration dies, so a test-only database/sql driver (sql.OpenDB + a
// connector, no global registration) supplies rows that exhaust and then
// error. This touches no production abstraction and no network.
func TestAppNamesMidIterationErrorReportsFailure(t *testing.T) {
	db := sql.OpenDB(stubNamesConnector{rows: &stubNameRows{rows: [][]driver.Value{
		{"alpha"},
		{"beta"},
	}}})
	defer db.Close()
	st := &State{db: db, log: fallbackLogger}

	names, ok := st.AppNames(context.Background())
	if ok {
		t.Fatal("a read that fails mid-iteration must report ok=false, not a partial success")
	}
	// The partial set (alpha, beta were successfully scanned) must not leak:
	// the heap slice does not survive as a usable answer.
	if len(names) != 0 {
		t.Fatalf("AppNames after mid-iteration error = %v, want no usable partial set (alpha/beta must be discarded)", names)
	}
	// Per the documented contract the failure value is nil specifically.
	if !reflect.ValueOf(names).IsNil() {
		t.Fatalf("AppNames after mid-iteration error = %v, want nil", names)
	}
}

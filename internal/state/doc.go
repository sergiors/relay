// Package state persists a read-only view of Relay's function state in a
// local SQLite database.
//
// The state database is a state VIEW, not the source of truth: /functions (the
// filesystem root the loader reads) is authoritative. The database is rebuilt
// automatically when empty and never drives matching, image building, or
// reconciliation decisions. It exists so operators can introspect what Relay
// has loaded and how the last reconcile of each function went, independent of
// Redis or Docker.
//
// State Model:
//   - status: "ready" (an active version is built and serving) or "pending"
//     (loaded but not yet built/verified)
//   - last_reconcile_status: "success" | "failed" | "skipped"
//
// A failed reconcile never marks a whole function unavailable: the previously
// active image/fingerprint/prepared_at are retained so the last good version
// still serves. All timestamps are RFC3339 strings.
//
// Design Constraint:
//   - State errors are never fatal. Callers (main, reconciler) log them and
//     continue; the state database degrades to "no state available" on failure.
//
// The driver is modernc.org/sqlite (pure Go, CGO-free) so the binary stays
// static under CGO_ENABLED=0 and the CLI is fully read-only with no external
// dependencies.
package state

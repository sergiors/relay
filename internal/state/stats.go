package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrStatsOwnershipLost reports that a flush did not own its stats row: another
// process incarnation of the same node has claimed it since this handle
// opened, so the snapshot was deliberately NOT written. The caller should stop
// writing under this incarnation rather than retry.
var ErrStatsOwnershipLost = errors.New("stats ownership lost")

// ErrStatsResetAdvanced reports that a stats reset epoch advanced since the
// caller's last observation — either the global epoch (a `stats reset --all`)
// or this node's local epoch (a `stats reset` for the same node), possibly in
// another process. The snapshot was NOT written. The caller must rebase its
// cumulative baseline to its current registry values, adopt the generations
// returned alongside this error, and retry once.
var ErrStatsResetAdvanced = errors.New("stats reset advanced")

// StatsGenerationUnknown is the sentinel for "this process has not observed a
// reset epoch yet". A negative generation lets the first snapshot adopt the
// stored epoch instead of treating it as a reset race.
const StatsGenerationUnknown int64 = -1

// StatsGenerations is the pair of reset epochs a stats snapshot is written
// under: Global is the shared epoch bumped by `stats reset --all`, Node is the
// per-node epoch bumped by `stats reset` for one node. A flush is fenced by
// both; a negative value means unknown and adopts the stored epoch.
type StatsGenerations struct {
	Global int64
	Node   int64
}

// Stats is the current operational snapshot of Relay's runtime, persisted as the
// JSONB payload of a per-node row in the stats table (keyed by node identity).
// The monotonic counters (events received/matched/unmatched, handler
// success/failure, retries, dlq) survive restarts and always reflect the latest
// known totals; the gauge fields (pending_entries, oldest_pending_age) are
// point-in-time snapshots of the backlog and are replaced on every record.
//
// The three event counters form a closed partition of the logical incoming
// events the runner handled: EventsReceivedTotal == EventsMatchedTotal +
// EventsUnmatchedTotal. Each logical event is classified exactly once across
// redeliveries and retries; schedule occurrences are excluded (they bypass
// event matching).
//
// Ownership: each node writes only its OWN row (State.nodeID), so N nodes
// sharing one PostgreSQL database keep N independent counter sets instead of
// last-writer-wins overwriting a single global row. A node's startup restore
// reads only its own row (OwnerStats); an operator read selects one node
// (ReadNodeStats) or aggregates every node (ReadStats) via mergeStats.
//
// The backlog gauges carry their own successful-observation instant
// (PendingObservedAtUnixMs). updated_at is only the row's last write time, and a
// failed XPENDING sample still advances it because the counters are flushed; it
// therefore cannot order two rows' gauges. A zero observation instant means a
// legacy payload (or a caller that never observed the backlog) and is never
// treated as fresher than an observed pair.
//
// The struct is the source of truth for the payload: JSON (un)marshalling is
// centralized in stats_json.go and the stable relational metadata — the node
// key and updated_at — stays a column (UpdatedAt is excluded from the payload).
type Stats struct {
	EventsReceivedTotal     int64  `json:"events_received_total"`
	EventsMatchedTotal      int64  `json:"events_matched_total"`
	EventsUnmatchedTotal    int64  `json:"events_unmatched_total"`
	HandlerSuccessTotal     int64  `json:"handler_success_total"`
	HandlerFailureTotal     int64  `json:"handler_failure_total"`
	RetryTotal              int64  `json:"retry_total"`
	DLQTotal                int64  `json:"dlq_total"`
	PendingEntries          int64  `json:"pending_entries"`
	OldestPendingAgeSeconds int64  `json:"oldest_pending_age_seconds"`
	PendingObservedAtUnixMs int64  `json:"pending_observed_at_unix_ms,omitempty"`
	UpdatedAt               string `json:"-"`
}

// RecordStats upserts this worker's stats row with the values the caller passes.
// It is a thin wrapper over RecordStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (st *State) RecordStats(stats Stats) {
	st.RecordStatsContext(context.Background(), stats)
}

// RecordStatsContext upserts THIS worker's stats row with the values the caller
// passes; the whole JSONB payload is REPLACED with the marshalled struct and
// updated_at is set to now(). There is no accumulation: the worker hands over
// the CURRENT cumulative registry values for the counters and current gauge
// snapshots for the backlog, so the row always mirrors the latest known totals.
//
// The payload is written with PostgreSQL's jsonb type, which validates it at
// write time.
//
// Absolute snapshot semantics: each flush writes the caller's current
// cumulative values; a repeated flush with identical values is a no-op effect
// (same totals, refreshed updated_at), never double-counting. Callers must pass
// CURRENT cumulative values; the worker seeds the fresh process registry from
// this row at startup so the first snapshot never resets counters. It is
// non-fatal on error: it logs and returns.
func (st *State) RecordStatsContext(ctx context.Context, stats Stats) {
	payload, err := marshalStats(stats)
	if err != nil {
		st.log.Warn("State: record stats failed", "error", err)
		return
	}
	_, err = st.db.ExecContext(ctx,
		`INSERT INTO stats (node_id, data, updated_at) VALUES ($1, $2, $3)
		 ON CONFLICT(node_id) DO UPDATE SET
		   data       = excluded.data,
		   updated_at = excluded.updated_at`,
		st.nodeID, payload, st.nowTime().UTC())
	if err != nil {
		st.log.Warn("State: record stats failed", "error", err)
	}
}

// RecordStatsSnapshot persists the whole stats snapshot in ONE short
// transaction: it first prunes orphaned app_stats rows (rows whose
// app no longer has an apps row — an app removed by RecordRemoved
// must not keep a stats row), then upserts this worker's global stats row, then
// upserts each per-app row present in fns for THIS worker. The per-app upsert is
// conditional on the app still existing in the apps table, so a
// removed app's row is not re-created even if the caller's snapshot still
// reports its (now-stale) counters. This is the worker's flush path; it keeps
// the whole snapshot atomic and idempotent (absolute values, no deltas).
//
// FENCING. The snapshot is written only by the CURRENT ownership incarnation and
// only under the caller's known reset epochs (see
// RecordStatsSnapshotWithGenerations):
//
//   - The global generation is the caller's last-known shared epoch (see
//     StatsGeneration). A NEGATIVE value means "unknown, adopt current", which
//     lets a process's very first flush avoid a spurious rebase. If it is
//     non-negative and differs from the stored one, a `stats reset --all`
//     advanced the shared epoch, and the call returns
//     (currentGenerations, ErrStatsResetAdvanced) WITHOUT writing so the caller
//     can rebase and retry.
//   - The node generation is the caller's last-known epoch for THIS node (see
//     StatsGenerations). A local `stats reset` bumps only this row's epoch, so a
//     mismatch fences a stale snapshot for that node without affecting any other
//     node. A NEGATIVE value adopts an existing zero/absent epoch; an existing
//     non-zero epoch reports ErrStatsResetAdvanced so a first flush can never
//     overwrite a local reset it did not observe.
//   - The global row upsert is conditioned on stats.instance = this handle's
//     instance AND on the node generation read earlier, so a stale process
//     incarnation or a concurrently inserted reset row affects no rows.
//     Per-app rows are written only AFTER the global ownership check passes, so
//     an incarnation that lost ownership writes nothing at all.
//
// Both checks are decided at the START of the transaction: it first takes the
// stats_meta row FOR SHARE, then locks this node's stats row. A reset (global or
// node-local) takes the same rows in the same order, with the global reset
// holding stats_meta exclusively for its whole transaction, so no deadlock is
// possible against a concurrent reset or prune. Every path returns the stored
// generations so the caller knows the epochs to adopt. Unexpected errors are
// logged here (matching the package's non-fatal style); the two sentinel
// outcomes are control flow and are left to the caller.
//
// Timestamp preservation: the four per-app Last*At fields are execution
// history, so an empty incoming value PRESERVES the persisted one rather than
// erasing it. Only THIS worker's stored payloads are read for the merge (the
// row is keyed by app+node), so a worker never inherits another worker's
// timestamps. Because each row's payload is JSONB, the stored payloads are read
// once inside the transaction and merged before writing (see
// mergeAppStatsTimestamps); counters are never merged — they are absolute
// snapshots and always overwrite.
// RecordStatsSnapshot is the global-epoch form of
// RecordStatsSnapshotWithGenerations, retained for callers that only track the
// shared reset epoch. The unknown node epoch adopts only a stored zero/absent
// epoch; a stored non-zero node epoch fences the snapshot with
// ErrStatsResetAdvanced. The production worker uses the explicit form so a local
// reset also fences its snapshots.
func (st *State) RecordStatsSnapshot(ctx context.Context, generation int64, stats Stats, fns []AppStats) (int64, error) {
	next, err := st.RecordStatsSnapshotWithGenerations(ctx, StatsGenerations{
		Global: generation,
		Node:   StatsGenerationUnknown,
	}, stats, fns)
	return next.Global, err
}

// RecordStatsSnapshotWithGenerations persists one snapshot under both the
// shared and the per-node reset epochs. It is the worker's flush path; see the
// RecordStatsSnapshot doc comment above for the snapshot and fencing contract.
func (st *State) RecordStatsSnapshotWithGenerations(
	ctx context.Context,
	generations StatsGenerations,
	stats Stats,
	fns []AppStats,
) (StatsGenerations, error) {
	globalPayload, err := marshalStats(stats)
	if err != nil {
		st.log.Warn("State: flush stats snapshot failed", "error", err)
		return generations, err
	}
	// current carries the stored generations out of the transaction on every
	// path; it defaults to the caller's values so a failure before a read leaves
	// the caller's epochs unchanged.
	current := generations
	err = st.rebuildTx(ctx, func(tx *sql.Tx) error {
		globalGen, err := readGenerationForWrite(ctx, tx)
		if err != nil {
			return err
		}
		current.Global = globalGen
		if generations.Global >= 0 && generations.Global != globalGen {
			return ErrStatsResetAdvanced
		}

		storedGen, storedInstance, found, err :=
			readNodeGenerationForWrite(ctx, tx, st.nodeID)
		if err != nil {
			return err
		}
		if found {
			current.Node = storedGen
		} else {
			current.Node = 0
		}
		if generations.Node >= 0 && generations.Node != current.Node {
			return ErrStatsResetAdvanced
		}
		// An unknown epoch may adopt an existing zero/absent epoch, but never a
		// non-zero one: that would let a first flush resurrect pre-reset history.
		if generations.Node < 0 && current.Node != 0 {
			return ErrStatsResetAdvanced
		}
		// An empty stored instance is an unowned row (a fresh local-reset row, or
		// an ownerless admin write); the current worker may claim it. A non-empty
		// different instance is a stale incarnation and is fenced.
		if found && storedInstance != "" && storedInstance != st.instance {
			return ErrStatsOwnershipLost
		}
		if !found && generations.Node > 0 {
			return ErrStatsResetAdvanced
		}

		// Prune orphaned app_stats rows first so a removed app's row
		// is gone before the upserts below could re-create it. The apps table is
		// shared and authoritative across workers, so an orphan is an orphan for
		// every node.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM app_stats WHERE app_name NOT IN (SELECT name FROM apps)`); err != nil {
			return err
		}
		// Read THIS worker's surviving stored payloads once, for the timestamp
		// merge.
		stored, err := st.storedAppStatsPayloads(ctx, tx)
		if err != nil {
			return err
		}
		// One timestamp for the whole snapshot, so this worker's global row and
		// every per-app row share the same updated_at.
		ts := st.nowTime().UTC()
		expectedNode := current.Node
		res, err := tx.ExecContext(ctx,
			`INSERT INTO stats (node_id, instance, generation, data, updated_at)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT(node_id) DO UPDATE SET
			   instance   = excluded.instance,
			   data       = excluded.data,
			   updated_at = excluded.updated_at
			 WHERE (stats.instance = excluded.instance OR stats.instance = '')
			   AND stats.generation = excluded.generation`,
			st.nodeID, st.instance, expectedNode, globalPayload, ts)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			// The row was claimed by another incarnation or a reset inserted a
			// newer node epoch between our read and this upsert. Re-read to
			// classify the failure for the caller's rebase/latch decision; skip
			// the per-app writes too so no partial snapshot lands.
			storedGen, storedInstance, found, err := readNodeGeneration(ctx, tx, st.nodeID)
			if err != nil {
				return err
			}
			if found {
				current.Node = storedGen
			}
			switch {
			case found && storedInstance != "" && storedInstance != st.instance:
				return ErrStatsOwnershipLost
			case !found || storedGen != expectedNode:
				return ErrStatsResetAdvanced
			default:
				return fmt.Errorf("stats snapshot not written")
			}
		}
		for _, fs := range fns {
			// Counters are absolute and always replaced; an empty incoming
			// timestamp is filled from the stored payload so a flush that
			// observed nothing never erases history.
			merged := mergeAppStatsTimestamps(stored[fs.App], fs)
			payload, err := marshalAppStats(merged)
			if err != nil {
				return err
			}
			// The EXISTS guard keeps a removed app's row from being
			// re-created even though the prune above already removed it. The
			// SELECT form lets the guard and the upsert stay one statement.
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO app_stats (app_name, node_id, data, updated_at)
				 SELECT $1, $2, $3, $4
				 WHERE EXISTS (SELECT 1 FROM apps WHERE name = $1)
				 ON CONFLICT(app_name, node_id) DO UPDATE SET
				   data       = excluded.data,
				   updated_at = excluded.updated_at`,
				fs.App, st.nodeID, payload, ts); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrStatsResetAdvanced) && !errors.Is(err, ErrStatsOwnershipLost) {
		st.log.Warn("State: flush stats snapshot failed", "error", err)
	}
	return current, err
}

// readGenerationForWrite reads the global reset generation inside tx under a
// SHARED row lock. The shared lock serializes a flush with a reset's exclusive
// bump; because both statements take the stats_meta row first, they can never
// deadlock against each other. The stats_meta seed row is created by
// initSchema, so it is always present.
func readGenerationForWrite(ctx context.Context, tx *sql.Tx) (int64, error) {
	var gen int64
	if err := tx.QueryRowContext(ctx,
		`SELECT generation FROM stats_meta WHERE id = 'global' FOR SHARE`).Scan(&gen); err != nil {
		return 0, fmt.Errorf("read stats generation: %w", err)
	}
	return gen, nil
}

// readNodeGenerationForWrite reads this node's local reset generation and
// owning incarnation inside tx, locking an existing row FOR UPDATE. Absence is
// reported as found=false with no error so the first flush can create the row.
// The row lock serializes the flush with a concurrent local reset (which takes
// the same row) while the caller already holds the shared stats_meta lock, so
// the lock order is always meta then stats.
func readNodeGenerationForWrite(
	ctx context.Context, tx *sql.Tx, nodeID string,
) (int64, string, bool, error) {
	return readNodeGenerationRow(ctx, tx, nodeID, true)
}

// readNodeGeneration is the unlocked re-read used to classify a failed
// conditional upsert, where a concurrent reset may have inserted a newer row.
func readNodeGeneration(
	ctx context.Context, tx *sql.Tx, nodeID string,
) (int64, string, bool, error) {
	return readNodeGenerationRow(ctx, tx, nodeID, false)
}

func readNodeGenerationRow(
	ctx context.Context, tx *sql.Tx, nodeID string, lock bool,
) (int64, string, bool, error) {
	query := `SELECT generation, instance FROM stats WHERE node_id = $1`
	if lock {
		query += ` FOR UPDATE`
	}
	var (
		gen      int64
		instance string
	)
	err := tx.QueryRowContext(ctx, query, nodeID).Scan(&gen, &instance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("read node stats generation: %w", err)
	}
	return gen, instance, true, nil
}

// StatsGenerations returns the current shared and node-local reset epochs. It is
// a direct read for diagnostics; startup uses OwnStatsSnapshot so the epochs are
// captured atomically with the restored data, and the flush path learns them
// from RecordStatsSnapshotWithGenerations instead.
func (st *State) StatsGenerations() (StatsGenerations, error) {
	ctx := context.Background()
	var generations StatsGenerations
	row := st.db.QueryRowContext(ctx,
		`SELECT generation FROM stats_meta WHERE id = 'global'`)
	if err := row.Scan(&generations.Global); err != nil {
		return StatsGenerations{}, fmt.Errorf("read stats generation: %w", err)
	}
	if st.nodeID == "" {
		generations.Node = StatsGenerationUnknown
		return generations, nil
	}
	var gen int64
	err := st.db.QueryRowContext(ctx,
		`SELECT generation FROM stats WHERE node_id = $1`, st.nodeID).Scan(&gen)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		generations.Node = 0
	case err != nil:
		return StatsGenerations{}, fmt.Errorf("read node stats generation: %w", err)
	default:
		generations.Node = gen
	}
	return generations, nil
}

// OwnStatsSnapshot is one node's persisted stats state captured atomically with
// the reset epochs it was written under. Restoring a fresh worker from this
// single snapshot is what makes startup safe against a reset racing the restore:
// seeding stale totals with a newer generation would let the first flush
// resurrect pre-reset history.
type OwnStatsSnapshot struct {
	Stats       Stats
	AppStats    []AppStats
	Generations StatsGenerations
}

// OwnStatsSnapshot reads THIS handle's own stats row, its app_stats rows, and the
// current global and node-local reset epochs in one transaction. It takes the
// stats_meta row FOR SHARE and then the node's stats row FOR SHARE, matching the
// flush/reset lock order, so a global reset cannot interleave with the snapshot
// and a node-local reset of this node cannot advance past it. A reset after the
// snapshot therefore advances an epoch the caller can detect; a reset before it
// is already reflected in the zeroed data and returned epochs.
//
// It is the startup restore path. Gauges are read back like any other payload
// field but are not seeded by the worker. A corrupt global payload is logged and
// treated as zero (matching OwnerStats); a corrupt app payload is logged and
// skipped (matching OwnerAppStats). Query/scan failures are returned.
func (st *State) OwnStatsSnapshot(ctx context.Context) (OwnStatsSnapshot, error) {
	var snapshot OwnStatsSnapshot
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		globalGen, err := readGenerationForWrite(ctx, tx)
		if err != nil {
			return err
		}
		snapshot.Generations.Global = globalGen

		var (
			nodeGen   int64
			data      sql.NullString
			updatedAt time.Time
		)
		err = tx.QueryRowContext(ctx,
			`SELECT generation, `+jsonPayloadExpr+`, updated_at
			 FROM stats WHERE node_id = $1 FOR SHARE`, st.nodeID,
		).Scan(&nodeGen, &data, &updatedAt)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			snapshot.Generations.Node = 0
		case err != nil:
			return fmt.Errorf("read own stats: %w", err)
		default:
			snapshot.Generations.Node = nodeGen
			parsed, decodeErr := unmarshalStats(data)
			if decodeErr != nil {
				st.log.Warn("State: read own stats failed", "error", decodeErr)
			} else {
				parsed.UpdatedAt = timestampString(updatedAt)
				snapshot.Stats = parsed
			}
		}

		rows, err := tx.QueryContext(ctx,
			`SELECT app_name, `+jsonPayloadExpr+`, updated_at
			 FROM app_stats WHERE node_id = $1 ORDER BY app_name`, st.nodeID)
		if err != nil {
			return fmt.Errorf("read own app stats: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				name         string
				appData      sql.NullString
				appUpdatedAt time.Time
			)
			if err := rows.Scan(&name, &appData, &appUpdatedAt); err != nil {
				return fmt.Errorf("scan own app stats: %w", err)
			}
			appStats, decodeErr := unmarshalAppStats(appData)
			if decodeErr != nil {
				st.log.Warn("State: read own app stats failed", "app", name, "error", decodeErr)
				continue
			}
			appStats.App = name
			appStats.UpdatedAt = timestampString(appUpdatedAt)
			snapshot.AppStats = append(snapshot.AppStats, appStats)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read own app stats: %w", err)
		}
		return nil
	})
	if err != nil {
		st.log.Warn("State: read own stats snapshot failed", "error", err)
		return OwnStatsSnapshot{}, err
	}
	return snapshot, nil
}

// StatsGeneration returns the current SHARED reset generation, or an error if it
// cannot be read. It is a direct read of the global epoch for diagnostics/tests;
// a caller that also needs the node-local epoch uses StatsGenerations, and the
// flush path learns both from RecordStatsSnapshotWithGenerations instead.
func (st *State) StatsGeneration() (int64, error) {
	ctx := context.Background()
	var gen int64
	if err := st.db.QueryRowContext(ctx,
		`SELECT generation FROM stats_meta WHERE id = 'global'`).Scan(&gen); err != nil {
		return 0, fmt.Errorf("read stats generation: %w", err)
	}
	return gen, nil
}

// storedAppStatsPayloads reads THIS worker's app_stats row payloads, keyed by
// app name, inside tx. A row whose JSON is invalid is logged (naming the app and
// the decode error) and treated as the zero value, so one corrupt row cannot
// abort the whole flush; the flush below overwrites it with the incoming
// absolute snapshot, self-healing the row.
func (st *State) storedAppStatsPayloads(ctx context.Context, tx *sql.Tx) (map[string]AppStats, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT app_name, `+jsonPayloadExpr+` FROM app_stats WHERE node_id = $1`, st.nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]AppStats)
	for rows.Next() {
		var name string
		var data sql.NullString
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		fs, err := unmarshalAppStats(data)
		if err != nil {
			st.log.Warn("State: read app stats failed", "app", name, "error", err)
			continue
		}
		out[name] = fs
	}
	return out, rows.Err()
}

// storedAppStatsTx reads and decodes THIS worker's app_stats row payload inside
// tx, returning the zero value when the row is absent. An invalid payload is
// logged (naming the app and the decode error) and treated as the zero value, so
// the caller's incoming absolute snapshot still lands and self-heals the row. It
// is the single-row companion of storedAppStatsPayloads, shared by the
// standalone upsert.
func (st *State) storedAppStatsTx(ctx context.Context, tx *sql.Tx, name string) AppStats {
	var data sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT `+jsonPayloadExpr+` FROM app_stats WHERE app_name = $1 AND node_id = $2`, name, st.nodeID,
	).Scan(&data)
	if err == sql.ErrNoRows {
		return AppStats{}
	}
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}
	}
	fs, err := unmarshalAppStats(data)
	if err != nil {
		st.log.Warn("State: read app stats failed", "app", name, "error", err)
		return AppStats{}
	}
	return fs
}

// ResetStats returns the persisted cumulative statistics to their
// fresh-install state in ONE transaction while PRESERVING the shape of the
// stored rows: every worker's global stats row and every per-app app_stats row
// survive, decoded from their typed JSON payloads. The global rows' seven
// cumulative counters (events received/matched/unmatched and
// handler success/handler failure/retry/DLQ) are zeroed; each app row's
// event-matched/handler counters plus the cumulative
// warm-acquire/cold-start/discarded pool counters are zeroed, and its four
// Last*At execution-history timestamps are cleared. The rows themselves are
// rewritten in place (an UPDATE, never a DELETE), so a running worker's flush
// simply writes new absolute values over them instead of re-creating them.
//
// The reset affects EVERY owner: the persisted history is a shared operator
// view, so `relay stats reset --all` clears all workers' rows (a running worker
// resets its in-memory source over the socket; the CLI fallback resets the whole
// database). The node-scoped counterpart is ResetNodeStats, which clears and
// fences only one node.
//
// The global backlog gauges (pending_entries / oldest_pending_age_seconds) are
// POINT-IN-TIME snapshots of the live Redis backlog, deliberately NOT reset:
// zeroing them would misreport a backlog that still exists — the next worker
// flush replaces them with fresh values anyway. Fields the typed marshal model
// does not know about are not round-tripped (the typed structs are the source
// of truth); every KNOWN unrelated field survives. updated_at is refreshed to
// now() on every rewritten row (generic last-write metadata, same semantics as
// RecordStats). Everything else in the database (apps and their
// snapshots, git state, secrets, invocation state) is untouched; no Prometheus
// counter, Redis state, worker, or container is involved.
//
// Concurrency: one transaction (rebuildTx), so a partial reset never lands. The
// in-memory metrics registry is NOT touched here — a running worker must reset
// its own in-memory source under its flush mutex so a captured pre-reset
// snapshot cannot be written after this reset (see worker's stats resetter);
// this method is the complete reset for a stopped worker.
//
// FENCING. The transaction FIRST bumps the global reset generation (an
// exclusive UPDATE of stats_meta, see bumpGeneration) and only then zeroes the
// rows, so the epoch change and the row rewrite commit atomically. Every flush
// takes the same stats_meta row FOR SHARE at the start of its own transaction,
// so an in-flight flush either completes entirely before this reset (and is
// then overwritten by the zeroing) or starts after it and observes the advanced
// epoch (returning ErrStatsResetAdvanced instead of resurrecting the reset
// history). This fences delayed snapshots from EVERY worker, not just the one
// that issued the reset: the generation is global, and a worker that captured a
// pre-reset snapshot refuses to write it under the new epoch. Bumping and
// zeroing in one transaction is required: a separate bump would let a rebasing
// worker write its fresh totals into rows this reset then zeroes, silently
// losing the counters that grew across the reset.
//
// A corrupt payload — global or per-app — is fatal: it cannot be decoded
// and rewritten to zero, so the decode error is returned and the transaction
// rolls back, leaving every row and the generation exactly as they were. A
// partial reset is never mistaken for a complete one.
//
// Returns the NEW global generation alongside the error; the generation is
// meaningful only when err is nil, since a failed reset rolls back the bump and
// leaves the stored epoch unchanged. The package's Warn log also records the
// failure, matching RecordStats.
func (st *State) ResetStats() (int64, error) {
	return st.ResetAllStats(context.Background())
}

// ResetAllStats is the context-aware form of the global reset. It bumps the
// shared reset epoch and zeroes every node's global and per-app counters in one
// transaction, so every worker is fenced and rebases its cumulative baseline on
// its next flush. See ResetStats for the full semantics.
func (st *State) ResetAllStats(ctx context.Context) (int64, error) {
	var gen int64
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		// Bump the epoch FIRST, taking the stats_meta row exclusively. Every
		// flush takes this row FOR SHARE before touching stats/app_stats, so
		// holding the exclusive lock for the whole reset orders the bump after
		// every in-flight flush and before every later one.
		var err error
		gen, err = bumpGeneration(ctx, tx, st.nowTime().UTC())
		if err != nil {
			return err
		}
		// One timestamp for every rewritten row, so every global row and all
		// per-app rows share it (the flush snapshot does the same).
		ts := st.nowTime().UTC()
		if err := st.resetGlobalStatsTx(ctx, tx, ts); err != nil {
			return err
		}
		return st.resetAppStatsTx(ctx, tx, ts, "")
	})
	if err != nil {
		st.log.Warn("State: reset stats failed", "error", err)
		return 0, err
	}
	return gen, nil
}

// ResetNodeStats zeroes ONE node's global and per-app cumulative counters and
// bumps only that node's local reset epoch, in one transaction. It returns the
// node's NEW epoch. Other nodes' rows and epochs, the shared stats_meta epoch,
// and every backlog gauge are untouched, so a local reset fences only stale
// snapshots from this node while `stats reset --all` remains the global fence.
//
// An absent stats row is CREATED with the new epoch and a zero payload, so a
// later first flush from that node sees the reset instead of writing an epoch it
// never observed. An existing row keeps its instance fencing value and its
// backlog gauges. The transaction takes stats_meta FOR SHARE first and then the
// node row, matching the flush and global-reset lock order; a concurrent first
// flush either lands before the reset (and is zeroed) or observes the new epoch
// and refuses to write. Corrupt payloads are fatal and roll the whole reset
// back.
func (st *State) ResetNodeStats(ctx context.Context, nodeID string) (int64, error) {
	if nodeID == "" {
		return 0, fmt.Errorf("reset node stats: node ID is required")
	}
	var gen int64
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		// Take the meta row FOR SHARE first, matching the flush and prune lock
		// order (meta before stats/app_stats), so this reset cannot deadlock
		// with a global reset's exclusive bump.
		var one int
		if err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM stats_meta WHERE id = 'global' FOR SHARE`).Scan(&one); err != nil {
			return err
		}
		zeroPayload, err := marshalStats(Stats{})
		if err != nil {
			return err
		}
		ts := st.nowTime().UTC()
		// Bump the epoch atomically whether the row exists or not. On conflict
		// only the epoch and updated_at change here; the payload is zeroed
		// below from the returned value so gauges and unknown fields survive.
		var data sql.NullString
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO stats (node_id, instance, generation, data, updated_at)
			 VALUES ($1, '', 1, $2, $3)
			 ON CONFLICT(node_id) DO UPDATE SET
			   generation = stats.generation + 1,
			   updated_at = excluded.updated_at
			 RETURNING generation, data::text`,
			nodeID, zeroPayload, ts).Scan(&gen, &data); err != nil {
			return fmt.Errorf("bump node stats generation: %w", err)
		}
		stats, err := unmarshalStats(data)
		if err != nil {
			return err
		}
		stats = zeroStatsCounters(stats)
		payload, err := marshalStats(stats)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE stats SET data = $1, updated_at = $2 WHERE node_id = $3`,
			payload, ts, nodeID); err != nil {
			return err
		}
		return st.resetAppStatsTx(ctx, tx, ts, nodeID)
	})
	if err != nil {
		st.log.Warn("State: reset node stats failed", "node_id", nodeID, "error", err)
		return 0, err
	}
	return gen, nil
}

// bumpGeneration increments the global reset generation inside tx and returns
// the new value. The UPDATE takes the stats_meta row exclusively, which is what
// serializes a reset against every flush (each flush takes the same row FOR
// SHARE): the single lock ordering — stats_meta first, then stats/app_stats —
// means a reset and a flush (or a prune) can never deadlock. The stats_meta
// seed row is created by initSchema, so the update always matches one row.
func bumpGeneration(ctx context.Context, tx *sql.Tx, ts time.Time) (int64, error) {
	var gen int64
	if err := tx.QueryRowContext(ctx,
		`UPDATE stats_meta SET generation = generation + 1, updated_at = $1
		 WHERE id = 'global' RETURNING generation`, ts).Scan(&gen); err != nil {
		return 0, fmt.Errorf("bump stats generation: %w", err)
	}
	return gen, nil
}

// PruneStaleStats deletes stats and app_stats rows owned by workers OTHER than
// this handle (st.nodeID) whose updated_at is older than retention, bounding the
// table growth from workers that no longer exist. It returns the total number of
// rows removed across both tables.
//
// A worker keeps its OWN row unconditionally (it is the live owner), even if its
// updated_at looks stale — a temporarily quiet worker must not lose the
// cumulative totals it will resume writing. Deleting a stats row also removes
// its node-local reset generation; stats_meta stays intact. Rows are matched by
// their native UTC updated_at values: a row is stale when
// updated_at < now-retention. The zero-time row a fresh ownership claim creates
// is therefore immediately stale for any OTHER worker, which is intended — an
// abandoned claim must not linger.
//
// Retention <= 0 is a no-op (compaction disabled) and deletes nothing. The whole
// prune runs in one transaction that takes the stats_meta row FOR SHARE first,
// so it serializes with a concurrent reset under the same lock ordering the
// flush path uses. This is a best-effort compaction, not part of the flush's
// correctness: a failure leaves rows in place and is reported to the caller.
func (st *State) PruneStaleStats(ctx context.Context, retention time.Duration) (int, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := st.nowTime().UTC().Add(-retention)
	var deleted int64
	err := st.rebuildTx(ctx, func(tx *sql.Tx) error {
		// Take the meta row FOR SHARE first, matching the flush path's lock
		// ordering (meta before stats/app_stats) so a prune cannot deadlock
		// with a reset's exclusive bump.
		var one int
		if err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM stats_meta WHERE id = 'global' FOR SHARE`).Scan(&one); err != nil {
			return err
		}
		for _, table := range []string{"stats", "app_stats"} {
			res, err := tx.ExecContext(ctx,
				`DELETE FROM `+table+` WHERE node_id <> $1 AND updated_at < $2`, st.nodeID, cutoff)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			deleted += n
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(deleted), nil
}

// zeroStatsCounters returns stats with its cumulative counters zeroed and every
// gauge, observation instant, and unknown decoded field preserved. Both the
// global and the node-local reset use it, so a reset never reports a backlog
// that still exists as zero.
func zeroStatsCounters(stats Stats) Stats {
	stats.EventsReceivedTotal = 0
	stats.EventsMatchedTotal = 0
	stats.EventsUnmatchedTotal = 0
	stats.HandlerSuccessTotal = 0
	stats.HandlerFailureTotal = 0
	stats.RetryTotal = 0
	stats.DLQTotal = 0
	return stats
}

// resetGlobalStatsTx zeroes every worker's stats row cumulative counters in
// tx while preserving the live backlog gauges and any other decoded field. An
// absent table (no rows) has nothing cumulative to zero and is left untouched
// (creating a row would falsely claim stats were recorded). A corrupt payload is
// returned as an error so the transaction rolls back.
func (st *State) resetGlobalStatsTx(ctx context.Context, tx *sql.Tx, ts time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT node_id, `+jsonPayloadExpr+` FROM stats`)
	if err != nil {
		return err
	}
	type globalRow struct {
		nodeID string
		data   sql.NullString
	}
	var pending []globalRow
	for rows.Next() {
		var r globalRow
		if err := rows.Scan(&r.nodeID, &r.data); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, r := range pending {
		// The column is NOT NULL, so a NULL rendering means the stored value is
		// not the typed payload; unmarshalStats surfaces that as a decode failure
		// (rolling the reset back) rather than silently zeroing a corrupt row.
		stats, err := unmarshalStats(r.data)
		if err != nil {
			return err
		}
		// Zero ONLY the cumulative counters; the two gauges and their
		// successful-observation instant are live backlog snapshots and are
		// preserved (the next flush refreshes them).
		stats = zeroStatsCounters(stats)
		payload, err := marshalStats(stats)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE stats SET data = $1, updated_at = $2 WHERE node_id = $3`,
			payload, ts, r.nodeID); err != nil {
			return err
		}
	}
	return nil
}

// resetAppStatsTx zeroes app_stats rows' cumulative fields in tx with an UPDATE
// per row (never a DELETE): the event-matched/handler counters, the three
// warm-container pool counters, and the four Last*At timestamps. An empty
// nodeID covers every worker's rows (the global reset); a non-empty nodeID
// covers only that node's rows (a local reset). A corrupt payload returns the
// decode error, rolling the whole reset back. Timestamps are cleared (not
// merged) because a reset is an explicit erasure of execution history.
func (st *State) resetAppStatsTx(
	ctx context.Context, tx *sql.Tx, ts time.Time, nodeID string,
) error {
	type row struct {
		name   string
		nodeID string
		data   sql.NullString
	}
	query := `SELECT app_name, node_id, ` + jsonPayloadExpr + ` FROM app_stats`
	var args []any
	if nodeID != "" {
		query += ` WHERE node_id = $1`
		args = append(args, nodeID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.name, &r.nodeID, &r.data); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, r := range pending {
		fs, err := unmarshalAppStats(r.data)
		if err != nil {
			return err
		}
		fs = zeroAppStatsCounters(fs)
		payload, err := marshalAppStats(fs)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE app_stats SET data = $1, updated_at = $2 WHERE app_name = $3 AND node_id = $4`,
			payload, ts, r.name, r.nodeID); err != nil {
			return err
		}
	}
	return nil
}

// zeroAppStatsCounters returns fs with every cumulative per-app counter zeroed
// and its execution-history timestamps cleared. It is the shared reset shape for
// both the global and the node-local reset.
func zeroAppStatsCounters(fs AppStats) AppStats {
	fs.EventsMatchedTotal = 0
	fs.HandlerSuccessTotal = 0
	fs.HandlerFailureTotal = 0
	fs.RetryTotal = 0
	fs.DLQTotal = 0
	fs.WarmAcquiresTotal = 0
	fs.ColdStartsTotal = 0
	fs.DiscardedTotal = 0
	fs.LastExecutionAt = ""
	fs.LastSuccessAt = ""
	fs.LastFailureAt = ""
	fs.LastDLQAt = ""
	return fs
}

// ReadStats returns the operational snapshot AGGREGATED across every node's
// persisted row. ok is false only when no row has been recorded yet; a query,
// scan, or decode failure is returned as an error so a caller can distinguish
// an empty stats table from an unreadable one instead of rendering zeros.
// Cumulative counters are summed across nodes. The two backlog gauges (a shared
// Redis backlog each worker reports) come from the row with the newest
// successful backlog observation, chosen as a pair and never summed; a legacy
// row without an observation instant never wins on updated_at alone. UpdatedAt
// is the newest row write time.
func (st *State) ReadStats(ctx context.Context) (Stats, bool, error) {
	return st.readStats(ctx, "")
}

// ReadNodeStats returns ONE node's operational snapshot, including its own
// backlog gauge pair, without aggregating another node's counters. Its gauges
// are returned verbatim even for a legacy payload with no observation instant;
// freshness selection applies only to the cross-node aggregate. ok is false when
// that node has no row yet; query, scan, and decode failures are returned as
// errors. This is the explicit node-filtered read behind `relay stats` and
// `relay stats --node`.
func (st *State) ReadNodeStats(
	ctx context.Context, nodeID string,
) (Stats, bool, error) {
	if nodeID == "" {
		return Stats{}, false, fmt.Errorf("read node stats: node ID is required")
	}
	return st.readStats(ctx, nodeID)
}

// Stats is the best-effort form of ReadStats: it returns (zero, false) for an
// absent row and also for a read/decode failure (which is logged). Callers that
// must tell those apart use ReadStats.
func (st *State) Stats() (Stats, bool) {
	s, ok, err := st.ReadStats(context.Background())
	if err != nil {
		st.log.Warn("State: read stats failed", "error", err)
		return Stats{}, false
	}
	return s, ok
}

// OwnerStats returns THIS node's operational snapshot, or (zero, false) when
// this node has no row yet or the read fails (which is logged). It is the read a
// worker uses at startup to restore its own counters (never another node's), so
// restarts of the same identity continue its totals without double-counting a
// shared database.
func (st *State) OwnerStats() (Stats, bool) {
	s, ok, err := st.readStats(context.Background(), st.nodeID)
	if err != nil {
		st.log.Warn("State: read stats failed", "error", err)
		return Stats{}, false
	}
	return s, ok
}

// readStats reads and merges stats rows. An empty nodeID reads every row (the
// cross-node aggregate); a non-empty nodeID reads only that node's row.
func (st *State) readStats(ctx context.Context, nodeID string) (Stats, bool, error) {
	query := `SELECT node_id, ` + jsonPayloadExpr + `, updated_at FROM stats`
	var args []any
	if nodeID != "" {
		query += ` WHERE node_id = $1`
		args = append(args, nodeID)
	}
	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return Stats{}, false, fmt.Errorf("read stats: %w", err)
	}
	defer rows.Close()

	var (
		out  Stats
		seen bool
	)
	for rows.Next() {
		var (
			rowNodeID string
			data      sql.NullString
			updatedAt time.Time
		)
		if err := rows.Scan(&rowNodeID, &data, &updatedAt); err != nil {
			return Stats{}, false, fmt.Errorf("scan stats row: %w", err)
		}
		s, err := unmarshalStats(data)
		if err != nil {
			return Stats{}, false, fmt.Errorf("read stats for node %q: %w", rowNodeID, err)
		}
		s.UpdatedAt = timestampString(updatedAt)
		if nodeID != "" {
			// A node-specific read shows that row's own gauges verbatim,
			// including a legacy payload with no observation instant. Only the
			// cross-node aggregate applies freshness selection.
			out = s
		} else {
			mergeStats(&out, s)
		}
		seen = true
	}
	if err := rows.Err(); err != nil {
		return Stats{}, false, fmt.Errorf("read stats: %w", err)
	}
	if !seen {
		return Stats{}, false, nil
	}
	return out, true, nil
}

// mergeStats folds src into dst: cumulative counters are summed (each row is an
// independent worker's absolute total), while the two backlog gauges are NOT
// summed and never merged component-wise. Every worker snapshots the SAME shared
// Redis backlog, so the aggregate presents exactly ONE worker's complete
// observation pair — the one with the newest successful-observation instant
// (PendingObservedAtUnixMs). Row updated_at is deliberately NOT used: a failed
// XPENDING sample still flushes counters and advances updated_at, so it cannot
// order the gauges. A legacy/unobserved row (instant 0) supplies no aggregate
// gauges at all; if no row has a valid observation the aggregate retains zero
// gauges. Equally fresh observations are ordered by their complete pair
// (depth, then age), so the result is deterministic regardless of row order
// without synthesizing a pair from two observations. UpdatedAt itself still
// takes the newest row write time for display.
func mergeStats(dst *Stats, src Stats) {
	dst.EventsReceivedTotal += src.EventsReceivedTotal
	dst.EventsMatchedTotal += src.EventsMatchedTotal
	dst.EventsUnmatchedTotal += src.EventsUnmatchedTotal
	dst.HandlerSuccessTotal += src.HandlerSuccessTotal
	dst.HandlerFailureTotal += src.HandlerFailureTotal
	dst.RetryTotal += src.RetryTotal
	dst.DLQTotal += src.DLQTotal
	if dst.PendingObservedAtUnixMs == 0 {
		// An aggregate only carries gauges from a valid observation. Clear any
		// legacy/unobserved values already in dst so they can never survive as
		// a synthesized pair when no source has freshness metadata.
		dst.PendingEntries = 0
		dst.OldestPendingAgeSeconds = 0
	}
	if src.PendingObservedAtUnixMs > 0 {
		switch {
		case dst.PendingObservedAtUnixMs == 0 ||
			src.PendingObservedAtUnixMs > dst.PendingObservedAtUnixMs:
			dst.PendingEntries = src.PendingEntries
			dst.OldestPendingAgeSeconds = src.OldestPendingAgeSeconds
			dst.PendingObservedAtUnixMs = src.PendingObservedAtUnixMs
		case src.PendingObservedAtUnixMs == dst.PendingObservedAtUnixMs &&
			gaugePairAfter(src, *dst):
			dst.PendingEntries = src.PendingEntries
			dst.OldestPendingAgeSeconds = src.OldestPendingAgeSeconds
		}
	}
	dst.UpdatedAt = maxRFC3339(dst.UpdatedAt, src.UpdatedAt)
}

// gaugePairAfter reports whether a's complete backlog pair sorts after b's:
// depth first, then oldest age. It is the deterministic tie-break for two
// observations carrying the same instant, never a component-wise merge.
func gaugePairAfter(a, b Stats) bool {
	if a.PendingEntries != b.PendingEntries {
		return a.PendingEntries > b.PendingEntries
	}
	return a.OldestPendingAgeSeconds > b.OldestPendingAgeSeconds
}

// maxRFC3339 returns the later of two RFC3339 UTC strings. RFC3339 UTC strings
// are lexicographically ordered, and the empty string ("never") sorts first.
func maxRFC3339(a, b string) string {
	if b > a {
		return b
	}
	return a
}

// mergeAppStatsInto folds src into dst for one app: cumulative counters are
// summed across workers, and the four Last*At execution-history timestamps take
// the newest observation. UpdatedAt takes the newest.
func mergeAppStatsInto(dst *AppStats, src AppStats) {
	dst.EventsMatchedTotal += src.EventsMatchedTotal
	dst.HandlerSuccessTotal += src.HandlerSuccessTotal
	dst.HandlerFailureTotal += src.HandlerFailureTotal
	dst.RetryTotal += src.RetryTotal
	dst.DLQTotal += src.DLQTotal
	dst.WarmAcquiresTotal += src.WarmAcquiresTotal
	dst.ColdStartsTotal += src.ColdStartsTotal
	dst.DiscardedTotal += src.DiscardedTotal
	dst.LastExecutionAt = maxRFC3339(dst.LastExecutionAt, src.LastExecutionAt)
	dst.LastSuccessAt = maxRFC3339(dst.LastSuccessAt, src.LastSuccessAt)
	dst.LastFailureAt = maxRFC3339(dst.LastFailureAt, src.LastFailureAt)
	dst.LastDLQAt = maxRFC3339(dst.LastDLQAt, src.LastDLQAt)
	dst.UpdatedAt = maxRFC3339(dst.UpdatedAt, src.UpdatedAt)
}

// sortAppStats orders app stats by app name, so aggregated reads are
// deterministic (the SQL ORDER BY the previous queries carried).
func sortAppStats(out []AppStats) {
	sort.Slice(out, func(i, j int) bool { return out[i].App < out[j].App })
}

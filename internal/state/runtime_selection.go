package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
)

// ErrRuntimeImageSelectionConflict reports that a compare-and-swap on a runtime
// image selection lost its race: the row was created or advanced to a different
// generation than the caller expected. It is distinct from a PostgreSQL error so
// callers can handle a lost race separately from a database failure.
var ErrRuntimeImageSelectionConflict = errors.New("runtime image selection generation conflict")

// runtimeSelectionLockNamespace is the high-order key of the transaction-scoped
// per-runtime advisory locks. It partitions them from appLockNamespace (1),
// dependencyImageLockNamespace (2), and schemaLockKey (the single-bigint
// 0x72656c6179), so a runtime ID can never alias an app, a dependency image, or
// the schema-init lock. Every selection CAS write and every runtime publication
// check takes this lock for the SAME runtime ID, across goroutines, pooled
// connections, and Relay processes; that is what makes an authoritative check
// and the publication it authorizes one atomic decision.
const runtimeSelectionLockNamespace int32 = 3

// runtimeSelectionLockKey maps a runtime ID to the low-order half of its
// advisory-lock key. A 32-bit hash collision merely over-serializes two unrelated
// runtimes and never affects correctness.
func runtimeSelectionLockKey(runtimeID string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(runtimeID))
	return int32(h.Sum32())
}

// lockRuntimeSelectionTx takes the transaction-scoped per-runtime advisory lock
// shared by CompareAndSwapRuntimeImageSelection and
// WithRuntimeImageSelectionLock. It serializes a selection write against any
// publication decision for the same runtime, including the row-absent case,
// which a row lock cannot guard. The lock is released when the enclosing
// transaction commits or rolls back.
func lockRuntimeSelectionTx(ctx context.Context, tx *sql.Tx, runtimeID string) error {
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1, $2)`,
		runtimeSelectionLockNamespace, runtimeSelectionLockKey(runtimeID)); err != nil {
		return fmt.Errorf("lock runtime image selection %q: %w", runtimeID, err)
	}
	return nil
}

// RuntimeImageSelection is one runtime's persisted base-image pin. RuntimeID
// is the template runtime identifier (e.g. "python3.14"); SourceImage is the
// EXACT configured base image reference for that runtime (e.g.
// "python:3.14-slim"); Digest is the immutable multi-platform index digest in
// the form "sha256:<64 lowercase hex>"; Generation is the monotonically
// increasing compare-and-swap generation (at least 1); and SelectedAt is the
// RFC3339 time the current selection was written.
//
// A missing row means "unpinned": the runtime behaves exactly as it did before
// image selections existed. This package deliberately does not know which
// runtime IDs Relay supports and never imports the runtime package; the runtime
// layer validates the ID/source-image binding before using a selection.
type RuntimeImageSelection struct {
	RuntimeID   string
	SourceImage string
	Digest      string
	Generation  int64
	SelectedAt  string
}

// ReadRuntimeImageSelections returns every persisted runtime image selection,
// ordered by runtime ID. An empty result means every runtime is unpinned. It is
// the snapshot read used by the worker at startup and by each periodic
// reconciliation pass to refresh the runtime Manager's validated view.
func (st *State) ReadRuntimeImageSelections(ctx context.Context) ([]RuntimeImageSelection, error) {
	rows, err := st.db.QueryContext(ctx,
		`SELECT runtime_id, source_image, digest, generation, selected_at
		 FROM runtime_image_selections ORDER BY runtime_id`)
	if err != nil {
		return nil, fmt.Errorf("read runtime image selections: %w", err)
	}
	defer rows.Close()

	var selections []RuntimeImageSelection
	for rows.Next() {
		var sel RuntimeImageSelection
		if err := rows.Scan(
			&sel.RuntimeID, &sel.SourceImage, &sel.Digest, &sel.Generation, &sel.SelectedAt,
		); err != nil {
			return nil, fmt.Errorf("read runtime image selections: %w", err)
		}
		selections = append(selections, sel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read runtime image selections: %w", err)
	}
	return selections, nil
}

// CompareAndSwapRuntimeImageSelection writes one runtime image selection only if
// its expected generation is still current. expectedGeneration 0 means "the
// runtime is unpinned/absent": the call inserts generation 1 and fails with
// ErrRuntimeImageSelectionConflict if a row already exists. A positive expected
// generation updates the row only when its stored generation equals that value,
// advancing it by one. The returned selection carries the written generation and
// the selection timestamp from State's injected clock.
//
// The write runs in a transaction holding the per-runtime advisory lock shared
// with WithRuntimeImageSelectionLock, so concurrent writers cannot both win the
// same expected generation and a runtime publication can never observe a
// selection between its authoritative read and a competing write. A conflict is
// returned via ErrRuntimeImageSelectionConflict; every database failure is a
// distinct error. It validates only the persisted shape (non-empty identifiers
// and a sha256:<64 lowercase hex> digest) and deliberately does not validate that
// the runtime ID exists or that SourceImage is that runtime's configured base
// image — that binding belongs to the runtime layer.
func (st *State) CompareAndSwapRuntimeImageSelection(
	ctx context.Context,
	selection RuntimeImageSelection,
	expectedGeneration int64,
) (RuntimeImageSelection, error) {
	if selection.RuntimeID == "" {
		return RuntimeImageSelection{}, fmt.Errorf("runtime image selection: runtime ID is empty")
	}
	if selection.SourceImage == "" {
		return RuntimeImageSelection{}, fmt.Errorf(
			"runtime image selection %q: source image is empty", selection.RuntimeID)
	}
	if !validRuntimeImageDigest(selection.Digest) {
		return RuntimeImageSelection{}, fmt.Errorf(
			"runtime image selection %q: digest %q must be sha256:<64 lowercase hex>",
			selection.RuntimeID, selection.Digest)
	}
	if expectedGeneration < 0 {
		return RuntimeImageSelection{}, fmt.Errorf(
			"runtime image selection %q: expected generation must be >= 0", selection.RuntimeID)
	}

	selectedAt := st.nowString()
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return RuntimeImageSelection{}, fmt.Errorf(
			"begin runtime image selection %q: %w", selection.RuntimeID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockRuntimeSelectionTx(ctx, tx, selection.RuntimeID); err != nil {
		return RuntimeImageSelection{}, err
	}

	if expectedGeneration == 0 {
		err = tx.QueryRowContext(ctx,
			`INSERT INTO runtime_image_selections
				(runtime_id, source_image, digest, generation, selected_at)
			 VALUES ($1, $2, $3, 1, $4)
			 ON CONFLICT (runtime_id) DO NOTHING
			 RETURNING generation`,
			selection.RuntimeID, selection.SourceImage, selection.Digest, selectedAt,
		).Scan(&selection.Generation)
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeImageSelection{}, fmt.Errorf(
				"runtime image selection %q: %w", selection.RuntimeID, ErrRuntimeImageSelectionConflict)
		}
		if err != nil {
			return RuntimeImageSelection{}, fmt.Errorf(
				"insert runtime image selection %q: %w", selection.RuntimeID, err)
		}
	} else {
		err = tx.QueryRowContext(ctx,
			`UPDATE runtime_image_selections
			 SET source_image = $2, digest = $3, generation = generation + 1, selected_at = $4
			 WHERE runtime_id = $1 AND generation = $5
			 RETURNING generation`,
			selection.RuntimeID, selection.SourceImage, selection.Digest, selectedAt, expectedGeneration,
		).Scan(&selection.Generation)
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeImageSelection{}, fmt.Errorf(
				"runtime image selection %q: %w", selection.RuntimeID, ErrRuntimeImageSelectionConflict)
		}
		if err != nil {
			return RuntimeImageSelection{}, fmt.Errorf(
				"update runtime image selection %q: %w", selection.RuntimeID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return RuntimeImageSelection{}, fmt.Errorf(
			"commit runtime image selection %q: %w", selection.RuntimeID, err)
	}
	selection.SelectedAt = selectedAt
	return selection, nil
}

// WithRuntimeImageSelectionLock runs fn while holding the transaction-scoped
// per-runtime advisory lock shared with CompareAndSwapRuntimeImageSelection, and
// passes fn the CURRENT persisted selection for runtimeID (nil when the row is
// absent, i.e. the runtime is unpinned). It is the authoritative-check seam for
// runtime image publication: a caller that validates the row inside fn and then
// performs its publication in the same call cannot race a CAS write that would
// change the selection between the check and the publication. The lock is held
// only for fn's duration — never across an image build — and the read-only
// transaction ends immediately afterwards. Because nothing is written, a failed
// release after fn succeeded is not returned: it cannot invalidate an
// already-validated publication, and the lock is released when the session closes.
//
// fn must be quick, must not write through this transaction, and must not start
// another transaction on this handle. A nil runtime ID is rejected: an empty
// identity cannot be locked authoritatively.
func (st *State) WithRuntimeImageSelectionLock(
	ctx context.Context,
	runtimeID string,
	fn func(context.Context, *RuntimeImageSelection) error,
) error {
	if runtimeID == "" {
		return fmt.Errorf("runtime image selection: runtime ID is empty")
	}
	if fn == nil {
		return fmt.Errorf("runtime image selection %q: callback is nil", runtimeID)
	}
	tx, err := st.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin runtime image selection %q: %w", runtimeID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockRuntimeSelectionTx(ctx, tx, runtimeID); err != nil {
		return err
	}

	var current RuntimeImageSelection
	err = tx.QueryRowContext(ctx,
		`SELECT runtime_id, source_image, digest, generation, selected_at
		 FROM runtime_image_selections WHERE runtime_id = $1`,
		runtimeID,
	).Scan(&current.RuntimeID, &current.SourceImage, &current.Digest,
		&current.Generation, &current.SelectedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if err := fn(ctx, nil); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("read runtime image selection %q: %w", runtimeID, err)
	} else {
		if err := fn(ctx, &current); err != nil {
			return err
		}
	}
	// The transaction is READ-ONLY: its only job is to hold the advisory lock
	// across the authoritative read and the caller's publication. Rolling back
	// (rather than committing) releases that lock without inventing a commit
	// failure AFTER the registry mutation has already happened. A rollback error
	// still releases the lock when the connection closes and cannot change any
	// persisted row, so the validated publication outcome stands.
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		st.log.Debug("Runtime image selection lock release failed",
			"runtime", runtimeID, "error", err)
	}
	return nil
}

// validRuntimeImageDigest reports whether digest is exactly the persisted form
// of a Docker multi-platform index digest: the "sha256:" prefix followed by 64
// lowercase hexadecimal characters. It is the same shape the table's CHECK
// constraint enforces.
func validRuntimeImageDigest(digest string) bool {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) || len(digest) != len(prefix)+64 {
		return false
	}
	for _, c := range digest[len(prefix):] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

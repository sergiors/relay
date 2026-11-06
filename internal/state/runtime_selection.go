package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrRuntimeImageSelectionConflict reports that a compare-and-swap on a runtime
// image selection lost its race: the row was created or advanced to a different
// generation than the caller expected. It is distinct from a PostgreSQL error so
// callers can handle a lost race separately from a database failure.
var ErrRuntimeImageSelectionConflict = errors.New("runtime image selection generation conflict")

// RuntimeImageSelection is one runtime's persisted startup image pin. RuntimeID
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
// the startup read used by the worker; there is no polling or live refresh.
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
// It is a single atomic statement, so concurrent writers cannot both win the
// same expected generation. A conflict is returned via
// ErrRuntimeImageSelectionConflict; every database failure is a distinct error.
// It validates only the persisted shape (non-empty identifiers and a
// sha256:<64 lowercase hex> digest) and deliberately does not validate that the
// runtime ID exists or that SourceImage is that runtime's configured base image
// — that binding belongs to the runtime layer.
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
	if expectedGeneration == 0 {
		err := st.db.QueryRowContext(ctx,
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
		selection.SelectedAt = selectedAt
		return selection, nil
	}

	err := st.db.QueryRowContext(ctx,
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
	selection.SelectedAt = selectedAt
	return selection, nil
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

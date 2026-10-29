package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"relay/internal/dlq"
)

// persistedDLQEntry is the private JSON representation stored in dlq.data.
// Event, Reason, and the broker diagnostic identity fields are byte slices so
// encoding/json base64-encodes them: JSONB rejects U+0000, and encoding/json
// silently replaces invalid UTF-8 in plain strings. A dead-lettered payload,
// handler error, or arbitrary Redis source/group/message name must therefore be
// preserved as bytes. Decoding restores the exact bytes.
type persistedDLQEntry struct {
	Source          []byte `json:"source"`
	SourceGroup     []byte `json:"source_group"`
	SourceMessageID []byte `json:"source_message_id"`
	Consumer        []byte `json:"consumer"`
	App             string `json:"app"`
	Handler         string `json:"handler"`
	Event           []byte `json:"event"`
	Reason          []byte `json:"reason"`
	Deliveries      int64  `json:"deliveries"`
	HandlerAttempts int    `json:"handler_attempts"`
	Trace           string `json:"trace,omitempty"`
}

// marshalDLQData encodes one public entry into the private JSONB payload. The
// relational columns carry only the stable ID, delivery identity, settlement
// marker, and write time.
func marshalDLQData(entry dlq.Entry) (string, error) {
	b, err := json.Marshal(persistedDLQEntry{
		Source:          []byte(entry.Source),
		SourceGroup:     []byte(entry.SourceGroup),
		SourceMessageID: []byte(entry.SourceMessageID),
		Consumer:        []byte(entry.Consumer),
		App:             entry.App,
		Handler:         entry.Handler,
		Event:           []byte(entry.Event),
		Reason:          []byte(entry.Reason),
		Deliveries:      entry.Deliveries,
		HandlerAttempts: entry.HandlerAttempts,
		Trace:           entry.Trace,
	})
	if err != nil {
		return "", fmt.Errorf("marshal DLQ entry %s: %w", entry.ID, err)
	}
	return string(b), nil
}

// unmarshalDLQData decodes one private JSONB payload into the public model.
func unmarshalDLQData(id, text string) (dlq.Entry, error) {
	var payload persistedDLQEntry
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return dlq.Entry{}, fmt.Errorf("unmarshal DLQ entry %s: %w", id, err)
	}
	return dlq.Entry{
		ID:              id,
		Source:          string(payload.Source),
		SourceGroup:     string(payload.SourceGroup),
		SourceMessageID: string(payload.SourceMessageID),
		Consumer:        string(payload.Consumer),
		Event:           string(payload.Event),
		Reason:          string(payload.Reason),
		App:             payload.App,
		Handler:         payload.Handler,
		Deliveries:      payload.Deliveries,
		HandlerAttempts: payload.HandlerAttempts,
		Trace:           payload.Trace,
	}, nil
}

// SaveDLQEntry persists one dead-letter entry idempotently by its stable ID.
// inserted=false means an entry with the same logical source delivery and
// invocation already exists, so its original payload and settlement state are
// preserved untouched. A new row starts UNSETTLED: only a confirmed source ACK
// (or reconciliation) may mark it settled.
//
// The stream layer writes this row BEFORE acknowledging the source delivery, so
// a committed row means the dead letter is durable even if the acknowledgement
// reply is lost.
func (st *State) SaveDLQEntry(ctx context.Context, entry dlq.Entry) (bool, error) {
	if entry.ID == "" {
		return false, fmt.Errorf("save DLQ entry: empty entry ID")
	}
	deliveryID := entry.DeliveryID
	if deliveryID == "" {
		deliveryID = dlq.NewDeliveryID(entry.Source, entry.SourceGroup, entry.SourceMessageID)
	}
	data, err := marshalDLQData(entry)
	if err != nil {
		return false, err
	}
	createdAt, err := st.dlqCreatedAt(entry.Timestamp)
	if err != nil {
		return false, err
	}
	res, err := st.db.ExecContext(ctx, `
		INSERT INTO dlq (id, data, delivery_id, settled_at_ms, created_at_ms)
		VALUES ($1, $2, $3, NULL, $4)
		ON CONFLICT (id) DO NOTHING`,
		entry.ID, data, deliveryID, createdAt.UnixMilli(),
	)
	if err != nil {
		return false, fmt.Errorf("save DLQ entry %s: %w", entry.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("save DLQ entry %s: rows affected: %w", entry.ID, err)
	}
	return n > 0, nil
}

// dlqCreatedAt resolves the entry timestamp, defaulting to the handle's clock
// when the caller supplied none. A malformed timestamp is refused rather than
// silently replaced, so a caller error surfaces instead of writing an entry at
// an unrelated time.
func (st *State) dlqCreatedAt(timestamp string) (time.Time, error) {
	if timestamp == "" {
		return st.nowTime().UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("DLQ entry timestamp %q: %w", timestamp, err)
	}
	return t.UTC(), nil
}

// MarkDLQDeliverySettled marks every row belonging to one logical source
// delivery settled. It is monotonic and only ever moves unsettled rows to
// settled; it is called after a confirmed source ACK or by reconciliation after
// the source broker reports the delivery is no longer pending.
func (st *State) MarkDLQDeliverySettled(ctx context.Context, deliveryID string, settledAt time.Time) error {
	if deliveryID == "" {
		return fmt.Errorf("mark DLQ delivery settled: empty delivery ID")
	}
	if _, err := st.db.ExecContext(ctx, `
		UPDATE dlq
		SET settled_at_ms = $1
		WHERE delivery_id = $2 AND settled_at_ms IS NULL`,
		settledAt.UTC().UnixMilli(), deliveryID,
	); err != nil {
		return fmt.Errorf("mark DLQ delivery %s settled: %w", deliveryID, err)
	}
	return nil
}

// ListUnsettledDLQDeliveries returns up to limit distinct source deliveries that
// still have unsettled rows, strictly after afterDeliveryID in stable
// delivery-ID order. Each row is returned with the decoded source identity the
// worker needs to ask the broker whether the delivery is still pending. A
// corrupt stored payload is logged and its delivery returned with an incomplete
// identity: the worker preserves it but still advances past it, so one bad row
// cannot starve later deliveries. An empty afterDeliveryID starts at the
// beginning; the worker owns wraparound.
func (st *State) ListUnsettledDLQDeliveries(ctx context.Context, afterDeliveryID string, limit int) ([]dlq.DeliveryRef, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := st.db.QueryContext(ctx, `
		SELECT DISTINCT ON (delivery_id) delivery_id, data::text
		FROM dlq
		WHERE settled_at_ms IS NULL AND delivery_id <> '' AND delivery_id > $1
		ORDER BY delivery_id, created_at_ms
		LIMIT $2`, afterDeliveryID, limit)
	if err != nil {
		return nil, fmt.Errorf("list unsettled DLQ deliveries: %w", err)
	}
	defer rows.Close()

	refs := make([]dlq.DeliveryRef, 0)
	for rows.Next() {
		var deliveryID, data string
		if err := rows.Scan(&deliveryID, &data); err != nil {
			return nil, fmt.Errorf("list unsettled DLQ deliveries: %w", err)
		}
		entry, err := unmarshalDLQData(deliveryID, data)
		if err != nil {
			// Preserve the delivery: without a decodable source identity it
			// cannot be checked or settled, but it must never be deleted on a
			// guess. Return the cursor-bearing ID so reconciliation still moves
			// past it and does not starve later deliveries.
			st.log.Warn("DLQ reconciliation: stored payload cannot be decoded; preserving unsettled",
				"delivery_id", deliveryID, "error", err)
			refs = append(refs, dlq.DeliveryRef{DeliveryID: deliveryID})
			continue
		}
		refs = append(refs, dlq.DeliveryRef{
			DeliveryID:      deliveryID,
			Source:          entry.Source,
			SourceGroup:     entry.SourceGroup,
			SourceMessageID: entry.SourceMessageID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unsettled DLQ deliveries: %w", err)
	}
	return refs, nil
}

// ListDLQEntries returns every dead-letter entry in write order (oldest first,
// with the stable ID as the tie-breaker for equal millisecond timestamps).
func (st *State) ListDLQEntries(ctx context.Context) ([]dlq.Entry, error) {
	rows, err := st.db.QueryContext(ctx, `
		SELECT id, data::text, delivery_id, settled_at_ms, created_at_ms
		FROM dlq
		ORDER BY created_at_ms, id`)
	if err != nil {
		return nil, fmt.Errorf("list DLQ entries: %w", err)
	}
	defer rows.Close()

	entries := make([]dlq.Entry, 0)
	for rows.Next() {
		entry, err := scanDLQEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("list DLQ entries: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list DLQ entries: %w", err)
	}
	return entries, nil
}

// GetDLQEntry returns the entry with the exact ID, or ok=false when it does not
// exist.
func (st *State) GetDLQEntry(ctx context.Context, id string) (dlq.Entry, bool, error) {
	row := st.db.QueryRowContext(ctx, `
		SELECT id, data::text, delivery_id, settled_at_ms, created_at_ms
		FROM dlq
		WHERE id = $1`, id)
	entry, err := scanDLQEntry(row)
	if err == sql.ErrNoRows {
		return dlq.Entry{}, false, nil
	}
	if err != nil {
		return dlq.Entry{}, false, fmt.Errorf("get DLQ entry %s: %w", id, err)
	}
	return entry, true, nil
}

// DeleteDLQEntry removes exactly the one SETTLED entry with id and reports
// whether it existed. An unsettled entry is still needed by delivery recovery,
// so it is refused with ErrEntryUnsettled and left in place. This method never
// touches broker invocation state and never causes execution.
func (st *State) DeleteDLQEntry(ctx context.Context, id string) (bool, error) {
	var settled sql.NullInt64
	err := st.db.QueryRowContext(ctx,
		`SELECT settled_at_ms FROM dlq WHERE id = $1`, id).Scan(&settled)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, err)
	}
	if !settled.Valid {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, dlq.ErrEntryUnsettled)
	}
	res, err := st.db.ExecContext(ctx,
		`DELETE FROM dlq WHERE id = $1 AND settled_at_ms IS NOT NULL`, id)
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: rows affected: %w", id, err)
	}
	return n > 0, nil
}

// PruneDLQEntries removes up to limit SETTLED entries whose write time is at or
// before cutoff and returns how many were deleted. Unsettled entries are never
// removed: their source delivery may still be recoverable and the row may be
// required by recovery. Each call is one bounded DELETE, so a large backlog
// drains across cycles instead of in one unbounded statement. A non-positive
// limit is a no-op.
func (st *State) PruneDLQEntries(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	res, err := st.db.ExecContext(ctx, `
		DELETE FROM dlq
		WHERE id IN (
			SELECT id FROM dlq
			WHERE settled_at_ms IS NOT NULL AND created_at_ms <= $1
			ORDER BY created_at_ms, id
			LIMIT $2
		)`,
		cutoff.UTC().UnixMilli(), limit)
	if err != nil {
		return 0, fmt.Errorf("prune DLQ entries: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune DLQ entries: rows affected: %w", err)
	}
	return int(n), nil
}

// ApproxDLQDepth returns PostgreSQL's planner estimate of the number of rows in
// the DLQ table (pg_class.reltuples). It is intentionally approximate and cheap:
// it is safe to call on the periodic metrics path, unlike a COUNT(*) scan. The
// estimate can lag recent inserts/deletes and read 0 for a table that has not
// been analyzed yet. Exact counts remain available through ListDLQEntries for
// operator commands, never on a hot path.
func (st *State) ApproxDLQDepth(ctx context.Context) (int64, error) {
	var depth int64
	if err := st.db.QueryRowContext(ctx, `
		SELECT GREATEST(reltuples, 0)::bigint
		FROM pg_class
		WHERE oid = 'dlq'::regclass`).Scan(&depth); err != nil {
		return 0, fmt.Errorf("approximate DLQ depth: %w", err)
	}
	return depth, nil
}

// dlqRow is the scan surface shared by *sql.Rows and *sql.Row.
type dlqRow interface {
	Scan(dest ...any) error
}

// scanDLQEntry reads one row into the broker-neutral entry model. The payload
// is decoded from the private JSONB representation; Timestamp/SettledAt come
// from the relational columns.
func scanDLQEntry(row dlqRow) (dlq.Entry, error) {
	var (
		id          string
		data        string
		deliveryID  string
		settledAtMs sql.NullInt64
		createdAtMs int64
	)
	if err := row.Scan(&id, &data, &deliveryID, &settledAtMs, &createdAtMs); err != nil {
		return dlq.Entry{}, err
	}
	entry, err := unmarshalDLQData(id, data)
	if err != nil {
		return dlq.Entry{}, err
	}
	entry.DeliveryID = deliveryID
	entry.Timestamp = time.UnixMilli(createdAtMs).UTC().Format(time.RFC3339)
	if settledAtMs.Valid {
		entry.SettledAt = time.UnixMilli(settledAtMs.Int64).UTC().Format(time.RFC3339)
	}
	return entry, nil
}

// The PostgreSQL handle is the production DLQ store. The assertion keeps the
// broker-neutral interface and the concrete implementation in sync.
var _ dlq.Store = (*State)(nil)

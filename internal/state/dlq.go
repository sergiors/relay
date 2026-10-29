package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"relay/internal/dlq"
)

// dbNowMillis is the database's current time as integer Unix milliseconds. All
// replay-claim comparisons use it so claim and expiry decisions are made by one
// authoritative clock, never the CLI's or a worker's. PostgreSQL's now() is the
// statement (or transaction) start time.
const dbNowMillis = `(EXTRACT(EPOCH FROM now()) * 1000)::bigint`

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
// so it is refused with ErrEntryUnsettled and left in place. An entry with an
// active replay claim is refused with ErrReplayInProgress: the replay may still
// be executing, and deleting its row would strand the claimant's fenced
// success delete. The row lock serializes this check against ClaimDLQReplay, so
// a claim cannot appear between the check and the delete. This method never
// touches broker invocation state and never causes execution.
func (st *State) DeleteDLQEntry(ctx context.Context, id string) (bool, error) {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: begin: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		settledAt  sql.NullInt64
		leaseUntil int64
		nowMs      int64
	)
	err = tx.QueryRowContext(ctx, `
		SELECT settled_at_ms, replay_lease_until_ms, `+dbNowMillis+`
		FROM dlq
		WHERE id = $1
		FOR UPDATE`, id).Scan(&settledAt, &leaseUntil, &nowMs)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, err)
	}
	if !settledAt.Valid {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, dlq.ErrEntryUnsettled)
	}
	if leaseUntil > nowMs {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, dlq.ErrReplayInProgress)
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM dlq WHERE id = $1 AND settled_at_ms IS NOT NULL`, id)
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: rows affected: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("delete DLQ entry %s: commit: %w", id, err)
	}
	return n > 0, nil
}

// ClaimDLQReplay atomically admits one replay of a settled entry. It locks the
// row, classifies it (missing / unsettled / actively claimed), increments the
// monotonic replay fencing token, and stamps a lease expiry in database time.
// The transaction commits BEFORE the caller contacts the worker, so the claim is
// durable without ever spanning an execution.
//
// A concurrent claimant sees the unexpired lease and gets ErrReplayInProgress
// without dialing the worker, so two CLI invocations can never both execute the
// same entry. An expired lease is claimable again: expiry is how a crashed or
// disconnected claimant recovers. Any database/commit error is returned
// unchanged and the caller must not contact the worker: the claim may or may not
// have committed, and waiting for the lease to expire is the only safe
// recovery.
func (st *State) ClaimDLQReplay(ctx context.Context, id string, lease time.Duration) (dlq.ReplayClaim, error) {
	if lease <= 0 {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: lease must be positive", id)
	}
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: begin: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		settledAt      sql.NullInt64
		currentToken   int64
		currentLeaseMs int64
		nowMs          int64
	)
	err = tx.QueryRowContext(ctx, `
		SELECT settled_at_ms, replay_claim_token, replay_lease_until_ms, `+dbNowMillis+`
		FROM dlq
		WHERE id = $1
		FOR UPDATE`, id).Scan(&settledAt, &currentToken, &currentLeaseMs, &nowMs)
	if errors.Is(err, sql.ErrNoRows) {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: %w", id, dlq.ErrEntryNotFound)
	}
	if err != nil {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: %w", id, err)
	}
	if !settledAt.Valid {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: %w", id, dlq.ErrEntryUnsettled)
	}
	if currentLeaseMs > nowMs {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: %w", id, dlq.ErrReplayInProgress)
	}

	token := currentToken + 1
	leaseUntilMs := nowMs + lease.Milliseconds()
	var (
		data         string
		deliveryID   string
		createdAtMs  int64
		claimedToken int64
		claimedUntil int64
	)
	err = tx.QueryRowContext(ctx, `
		UPDATE dlq
		SET replay_claim_token = $2, replay_lease_until_ms = $3
		WHERE id = $1
		RETURNING data::text, delivery_id, settled_at_ms, created_at_ms,
			replay_claim_token, replay_lease_until_ms`,
		id, token, leaseUntilMs,
	).Scan(&data, &deliveryID, &settledAt, &createdAtMs, &claimedToken, &claimedUntil)
	if err != nil {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: update: %w", id, err)
	}
	entry, err := unmarshalDLQData(id, data)
	if err != nil {
		return dlq.ReplayClaim{}, err
	}
	entry.DeliveryID = deliveryID
	entry.Timestamp = time.UnixMilli(createdAtMs).UTC().Format(time.RFC3339)
	if settledAt.Valid {
		entry.SettledAt = time.UnixMilli(settledAt.Int64).UTC().Format(time.RFC3339)
	}
	if err := tx.Commit(); err != nil {
		return dlq.ReplayClaim{}, fmt.Errorf("claim DLQ replay %s: commit: %w", id, err)
	}
	return dlq.ReplayClaim{
		Entry:      entry,
		Token:      claimedToken,
		LeaseUntil: time.UnixMilli(claimedUntil).UTC(),
	}, nil
}

// ReleaseDLQReplay releases a replay claim only if token is still current, so a
// superseded claimant cannot clear a newer claimant's lease. Releasing makes the
// entry immediately claimable again. A stale or already released claim matches
// no row and is a no-op; the lease it might otherwise have held is already
// expired or owned by someone else.
func (st *State) ReleaseDLQReplay(ctx context.Context, id string, token int64) error {
	if _, err := st.db.ExecContext(ctx, `
		UPDATE dlq
		SET replay_lease_until_ms = 0
		WHERE id = $1 AND replay_claim_token = $2`, id, token); err != nil {
		return fmt.Errorf("release DLQ replay %s: %w", id, err)
	}
	return nil
}

// DeleteDLQReplay removes an entry after a successful replay in ONE fenced
// conditional statement. It matches only a settled row whose current claim token
// is token AND whose lease is still unexpired at database time, so a claimant
// whose lease expired and was re-claimed (or whose row was recreated) cannot
// delete the row a newer claimant owns. A zero-row result is reported as
// ErrReplayClaimLost; the handler did run, so the entry is retained for the
// operator rather than guessed away. An execution error is returned unchanged
// and fails closed: the row is left for retention or a later operator action.
func (st *State) DeleteDLQReplay(ctx context.Context, id string, token int64) error {
	res, err := st.db.ExecContext(ctx, `
		DELETE FROM dlq
		WHERE id = $1
			AND settled_at_ms IS NOT NULL
			AND replay_claim_token = $2
			AND replay_lease_until_ms > `+dbNowMillis,
		id, token)
	if err != nil {
		return fmt.Errorf("delete DLQ replay %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete DLQ replay %s: rows affected: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("delete DLQ replay %s: %w", id, dlq.ErrReplayClaimLost)
	}
	return nil
}

// PruneDLQEntries removes up to limit SETTLED entries whose write time is at or
// before cutoff and returns how many were deleted. Unsettled entries are never
// removed: their source delivery may still be recoverable and the row may be
// required by recovery. Entries with an active replay claim are skipped too, so
// retention cannot delete the row out from under an in-flight replay; the lease
// expiry (or a later cycle) makes them eligible. Each call is one bounded
// DELETE, so a large backlog drains across cycles instead of in one unbounded
// statement. A non-positive limit is a no-op.
func (st *State) PruneDLQEntries(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	res, err := st.db.ExecContext(ctx, `
		DELETE FROM dlq
		WHERE id IN (
			SELECT id FROM dlq
			WHERE settled_at_ms IS NOT NULL
				AND replay_lease_until_ms <= `+dbNowMillis+`
				AND created_at_ms <= $1
			ORDER BY created_at_ms, id
			LIMIT $2
		)
			AND settled_at_ms IS NOT NULL
			AND replay_lease_until_ms <= `+dbNowMillis,
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
